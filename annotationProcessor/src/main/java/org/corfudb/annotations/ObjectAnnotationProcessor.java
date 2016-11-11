package org.corfudb.annotations;

import com.google.common.collect.ImmutableMap;
import com.squareup.javapoet.*;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
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

    private static final String CORFUSMR_SUFFIX = "$CORFUSMR";
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
                    .filter(x -> !x.getSimpleName().toString().endsWith(CORFUSMR_SUFFIX))
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
                + CORFUSMR_SUFFIX);
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
                .superclass(originalName);

        // Add the proxy field and an accessor, which manages the state of the object.
        typeSpecBuilder.addField(ParameterizedTypeName.get(ClassName.get(ICorfuSMRProxy.class),
                originalName), "proxy" + CORFUSMR_FIELD, Modifier.PUBLIC);
        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuSMRProxy")
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(ICorfuSMRProxy.class),
                            originalName))
                    .addStatement("return $L", "proxy" + CORFUSMR_FIELD)
                    .build());

        // Generate wrapper classes for @Accessor, @MutatorAccessor, @Mutator
        classElement.getEnclosedElements().stream()
                .filter(x -> x.getAnnotation(Accessor.class) != null ||
                        x.getAnnotation(MutatorAccessor.class) != null ||
                        x.getAnnotation(Mutator.class) != null
                )
                .map(x -> (ExecutableElement) x)
                .forEach(smrMethod -> {

                    // Extract each annotation
                    Accessor accessor = smrMethod.getAnnotation(Accessor.class);
                    MutatorAccessor mutatorAccessor = smrMethod.getAnnotation(MutatorAccessor.class);
                    Mutator mutator = smrMethod.getAnnotation(Mutator.class);

                    // Create a method methodName$CORFUSMR which lets us directly access the superclass.
                    /*
                    typeSpecBuilder.addMethod(
                            MethodSpec.methodBuilder(smrMethod.getSimpleName().toString() + CORFUSMR_SUFFIX)
                            .addModifiers(Modifier.PUBLIC)
                            .returns(ParameterizedTypeName.get(smrMethod.getReturnType()))
                            .addParameters(smrMethod.getParameters().stream()
                                            .map(x -> {
                                                Modifier[] m = x.getModifiers().toArray(new Modifier[
                                                        x.getModifiers().size()]);
                                                return ParameterSpec.builder(ParameterizedTypeName.get(x.asType()),
                                                    x.getSimpleName().toString(),m).build();
                                            }).collect(Collectors.toList()))
                            .addStatement("return super.$L($L)", smrMethod.getSimpleName(),
                                    smrMethod.getParameters().stream()
                                            .map(VariableElement::getSimpleName)
                                            .collect(Collectors.joining(", "))).build());
                    */
                    // Override the method we will proxy.
                    MethodSpec.Builder ms = MethodSpec.overriding(smrMethod);

                    // If a mutator, then log the update.
                    if (mutator != null || mutatorAccessor != null) {
                        ms.addStatement(
                                (mutatorAccessor != null ? "long address" + CORFUSMR_FIELD + " = " : "") +
                                "proxy" + CORFUSMR_FIELD + ".logSMRUpdate($S$L$L)",
                                getSMRFunctionName(smrMethod),
                                smrMethod.getParameters().size() > 0 ? "," : "",
                                smrMethod.getParameters().stream()
                                    .map(VariableElement::getSimpleName)
                                    .collect(Collectors.joining(", ")));
                    }


                    // If an accessor, return the object by doing the underlying call.
                    if (accessor != null || mutatorAccessor != null) {
                        ms.addStatement("return proxy" + CORFUSMR_FIELD + ".access(" +
                                (mutatorAccessor != null ? "address" + CORFUSMR_FIELD : "Long.MAX_VALUE") +
                                        ", o -> o.$L($L))",
                                smrMethod.getSimpleName(),
                                smrMethod.getParameters().stream()
                                        .map(VariableElement::getSimpleName)
                                        .collect(Collectors.joining(", ")));
                    }

                    typeSpecBuilder.addMethod(ms.build());
                });

        String upcallString = classElement.getEnclosedElements().stream()
                .filter(x -> x.getAnnotation(MutatorAccessor.class) != null ||
                            x.getAnnotation(Mutator.class) != null)
                .map(x -> (ExecutableElement) x)
                .map(x -> "\n.put(\"" + getSMRFunctionName(x) + "\", " +
                        "args -> { " + (x.getReturnType().getKind().equals(TypeKind.VOID)
                        ? "" : "return ") + "super." + x.getSimpleName() + "(" +
                        IntStream.range(0, x.getParameters().size())
                                .mapToObj(i ->
                                        "(" + x.getParameters().get(i).asType().toString() + ")" +
                                        " args[" + i + "]")
                                .collect(Collectors.joining(", "))
                        + ");" + (x.getReturnType().getKind().equals(TypeKind.VOID)
                        ? "return null;" : "") + "})")
                .collect(Collectors.joining());

        FieldSpec upcallMap = FieldSpec.builder(Map.class, "upcallMap" + CORFUSMR_FIELD,
                Modifier.PUBLIC, Modifier.FINAL)
                .initializer("new $T()$L.build()",
                        ParameterizedTypeName.get(ImmutableMap.Builder.class,
                                String.class,
                                ICorfuSMRUpcallTarget.class), upcallString)
                .build();

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
