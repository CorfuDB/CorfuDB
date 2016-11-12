package org.corfudb.annotations;

import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.*;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by mwei on 11/9/16.
 */
@SupportedAnnotationTypes("org.corfudb.annotations.*")
public class ObjectAnnotationProcessor extends AbstractProcessor {

    private Types typeUtils;
    private Elements elementUtils;
    private Filer filer;
    private Messager messager;

    // $ needs to be escaped, so we use _ for fields.
    private static final String CORFUSMR_FIELD = "_CORFUSMR";

    /** Always support the latest source version.
     *
     * @return  The source version supported.
     */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /**
     * {@inheritDoc}
     *
     * @param processingEnv
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        typeUtils = processingEnv.getTypeUtils();
        elementUtils = processingEnv.getElementUtils();
        filer = processingEnv.getFiler();
        messager = processingEnv.getMessager();
    }

    /**
     * {@inheritDoc}
     *
     * @param annotations
     * @param roundEnv
     */
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            roundEnv.getRootElements().stream()
                    .filter(x -> x.getKind() == ElementKind.CLASS)
                    // Don't re-instrument instrumented classes
                    .filter(x -> !x.getSimpleName().toString().endsWith(ICorfuSMR.CORFUSMR_SUFFIX))
                    .map(x -> (TypeElement) x)
                    .forEach(this::processClass);
        } catch (Exception e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        return true;
    }

    /** Process each class, checking if the class is annotated and
     * generating the appropiate proxy class.
     * @param classElement  The element to process.
     */
    public void processClass(TypeElement classElement) {
        // Does the class contain annotated elements? If so,
        // generate a new proxy file and process
        if (classElement.getEnclosedElements().stream()
                .filter(x -> x.getAnnotation(Accessor.class) != null)
                .count() > 0) {

            try {
                generateProxy(classElement);
            } catch (IOException e) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Failed to process class "
                        + classElement.getSimpleName() + " with IOException");
            }
        }
    }

    /** Return a SMR function name, extracting it from the annotations if available.
     *
     * @param smrMethod     The ExecutableElement to extract the name from.
     * @return              An SMR Function name string.
     */
    public String getSMRFunctionName(ExecutableElement smrMethod) {
        MutatorAccessor mutatorAccessor = smrMethod.getAnnotation(MutatorAccessor.class);
        Mutator mutator = smrMethod.getAnnotation(Mutator.class);
        return  (mutator != null && !mutator.name().equals("")) ? mutator.name() :
                (mutatorAccessor != null && !mutatorAccessor.name().equals("")) ?
                mutatorAccessor.name() : smrMethod.getSimpleName().toString();
    }

    /** Generate a proxy file.
     *
     * @param classElement  The element to generate a proxy file for
     * @throws IOException  If we could not generate the proxy file
     */
    public void generateProxy(TypeElement classElement)
            throws IOException
    {
        // Extract the package name for the class. We'll need this to generate the proxy file.
        String packageName = elementUtils.getPackageOf(classElement).toString();
        // Calculate the name of the proxy, which appends $CORFUSMR to the class name.
        ClassName proxyName = ClassName.bestGuess(classElement.getSimpleName().toString()
                + ICorfuSMR.CORFUSMR_SUFFIX);
        // Also get the class name of the original class.
        TypeName originalName = ParameterizedTypeName.get(classElement.asType());

        // Generate the proxy class. The proxy class extends the original class and implements
        // ICorfuSMR.
        TypeSpec.Builder typeSpecBuilder = TypeSpec
                .classBuilder(proxyName)
                .addTypeVariables(classElement.getTypeParameters()
                                    .stream()
                                    .map(TypeVariableName::get)
                                    .collect(Collectors.toList())
                )
                .addSuperinterface(ParameterizedTypeName
                        .get(ClassName.get(ICorfuSMR.class), originalName))
                .superclass(originalName)
                .addModifiers(Modifier.PUBLIC);

        // Generate a constructor. We assume that we have a no args constructor,
        // though that could change in the future.
        typeSpecBuilder.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC).build());

        // Add the proxy field and an accessor/setter, which manages the state of the object.
        typeSpecBuilder.addField(ParameterizedTypeName.get(ClassName.get(ICorfuSMRProxy.class),
                originalName), "proxy" + CORFUSMR_FIELD, Modifier.PUBLIC);
        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuSMRProxy")
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(ICorfuSMRProxy.class),
                            originalName))
                    .addStatement("return $L", "proxy" + CORFUSMR_FIELD)
                    .build());
        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("setCorfuSMRProxy")
                .addParameter(ParameterizedTypeName.get(ClassName.get(ICorfuSMRProxy.class),
                        originalName), "proxy")
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addStatement("this.$L = proxy", "proxy" + CORFUSMR_FIELD)
                .build());

        // Gather the set of methods for from this class.
        final Set<ExecutableElement> methodSet = classElement.getEnclosedElements().stream()
                .filter(x -> x.getKind() == ElementKind.METHOD)
                .map(x -> (ExecutableElement) x)
                .filter(x -> !x.getModifiers().contains(Modifier.STATIC)) // don't want static methods
                .collect(Collectors.toCollection(HashSet::new));

        // Deal with the inheritance tree for this class.
        TypeElement superClassElement = classElement;

        do {
            // Get the next superClass.
            superClassElement = ((TypeElement)((DeclaredType)superClassElement.getSuperclass()).asElement());
            boolean samePackage = ClassName.get(superClassElement).packageName().equals(
                                        ClassName.get(classElement).packageName());
            // If this method is not present in classElement, we need to override it.
            superClassElement.getEnclosedElements().stream()
                    .filter(x -> x.getKind() == ElementKind.METHOD)
                    .map(x -> (ExecutableElement) x)
                    .filter(x -> !x.getModifiers().contains(Modifier.FINAL)) // Can't override final
                    .filter(x -> !x.getModifiers().contains(Modifier.STATIC)) // or static...
                    //or protected, when we're not in the same package.
                    .filter(x -> samePackage || !x.getModifiers().contains(Modifier.PROTECTED))
                    //only public, or protected from same package filtered above
                    .filter(x -> x.getModifiers().contains(Modifier.PUBLIC) ||
                            x.getModifiers().contains(Modifier.PROTECTED))
                    .forEach(x -> {
                        if (methodSet.stream().noneMatch(y ->
                            // If this method is present in the parent, the string
                            // will match.
                            y.toString().equals(x.toString()))) {
                            methodSet.add(x);
                        }
                    });

            // Terminate if the current superClass is java.lang.Object.
        } while (!ClassName.get(superClassElement).equals(ClassName.OBJECT));



        // Generate wrapper classes.
        methodSet
                .forEach(smrMethod -> {

                    // Extract each annotation
                    Accessor accessor = smrMethod.getAnnotation(Accessor.class);
                    MutatorAccessor mutatorAccessor = smrMethod.getAnnotation(MutatorAccessor.class);
                    Mutator mutator = smrMethod.getAnnotation(Mutator.class);

                    // Override the method we will proxy.
                    MethodSpec.Builder ms = MethodSpec.overriding(smrMethod);

                    // This is a hack, but necessary since the modifier list is immutable
                    // and we need to remove the "native" modifier.
                    try {
                        Field f = ms.getClass().getDeclaredField("modifiers");
                        f.setAccessible(true);
                        ((List<Modifier>)f.get(ms)).remove(Modifier.NATIVE);
                    } catch (Exception e) {
                        messager.printMessage(Diagnostic.Kind.ERROR, "error trying to change methodspec"
                                + e.getMessage());
                    }

                    // If a mutator, then log the update.
                    if (mutator != null || mutatorAccessor != null) {
                        ms.addStatement(
                                (mutatorAccessor != null ? "long address" + CORFUSMR_FIELD + " = " : "") +
                                "proxy" + CORFUSMR_FIELD + ".logUpdate($S$L$L)",
                                getSMRFunctionName(smrMethod),
                                smrMethod.getParameters().size() > 0 ? "," : "",
                                smrMethod.getParameters().stream()
                                    .map(VariableElement::getSimpleName)
                                    .collect(Collectors.joining(", ")));
                    }


                    // If an accessor (or not annotated), return the object by doing the underlying call.
                    if (mutatorAccessor != null) {
                        // If the return to the mutatorAcessor is void, we don't need
                        // to do anything...
                        if (!smrMethod.getReturnType().getKind().equals(TypeKind.VOID)) {
                            ms.addStatement("return proxy" + CORFUSMR_FIELD + ".access(address"
                                    + CORFUSMR_FIELD + ", null)");
                        }
                    } else if (mutator == null) {
                        // Otherwise, just force the access to access the underlying call.
                        ms.addStatement((smrMethod.getReturnType().getKind().equals(TypeKind.VOID) ? "" :
                                        "return ") +"proxy" + CORFUSMR_FIELD + ".access(Long.MAX_VALUE" +
                                        ", o" + CORFUSMR_FIELD + " -> {$Lo" + CORFUSMR_FIELD + ".$L($L);$L})",
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID) ?
                                        "" : "return ",
                                smrMethod.getSimpleName(),
                                smrMethod.getParameters().stream()
                                        .map(VariableElement::getSimpleName)
                                        .collect(Collectors.joining(", ")),
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID) ?
                                        "return null;" : ""
                        );
                    }

                    typeSpecBuilder.addMethod(ms.build());
                });

        String upcallString = classElement.getEnclosedElements().stream()
                .filter(x -> x.getAnnotation(MutatorAccessor.class) != null ||
                            x.getAnnotation(Mutator.class) != null)
                .map(x -> (ExecutableElement) x)
                .map(x -> "\n.put(\"" + getSMRFunctionName(x) + "\", " +
                        "(obj, args) -> { " + (x.getReturnType().getKind().equals(TypeKind.VOID)
                        ? "" : "return ") + "obj." + x.getSimpleName() + "(" +
                        IntStream.range(0, x.getParameters().size())
                                .mapToObj(i ->
                                        "(" + x.getParameters().get(i).asType().toString() + ")" +
                                        " args[" + i + "]")
                                .collect(Collectors.joining(", "))
                        + ");" + (x.getReturnType().getKind().equals(TypeKind.VOID)
                        ? "return null;" : "") + "})")
                .collect(Collectors.joining());

        FieldSpec upcallMap = FieldSpec.builder(ParameterizedTypeName.get(ClassName.get(Map.class),
                ClassName.get(String.class),
                ParameterizedTypeName.get(ClassName.get(ICorfuSMRUpcallTarget.class),
                        originalName)), "upcallMap" + CORFUSMR_FIELD,
                Modifier.PUBLIC, Modifier.FINAL)
                .initializer("new $T()$L.build()",
                        ParameterizedTypeName.get(ClassName.get(ImmutableMap.Builder.class),
                                ClassName.get(String.class),
                                ParameterizedTypeName.get(ClassName.get(ICorfuSMRUpcallTarget.class),
                                originalName)), upcallString)
                .build();

        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuSMRUpcallMap")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ClassName.get(String.class),
                        ParameterizedTypeName.get(ClassName.get(ICorfuSMRUpcallTarget.class),
                                originalName)))
                .addStatement("return $L", "upcallMap" + CORFUSMR_FIELD)
                .build());

        typeSpecBuilder
                .addField(upcallMap);


        // Mark the object as instrumented, so we don't instrument it again.
        typeSpecBuilder
                .addAnnotation(AnnotationSpec.builder(InstrumentedCorfuObject.class).build());

        JavaFile javaFile = JavaFile
                .builder(packageName,
                        typeSpecBuilder.build())
                .build();

        javaFile.writeTo(filer);
    }
}
