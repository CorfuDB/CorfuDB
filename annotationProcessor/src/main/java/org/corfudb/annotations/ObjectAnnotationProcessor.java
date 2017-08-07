package org.corfudb.annotations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.IUndoFunction;
import org.corfudb.runtime.object.IUndoRecordFunction;

/** <p>The annotation processor, which takes annotated Corfu objects and
 * generates a class which can be used by the runtime instead of requiring
 * runtime instrumentation.</p>
 *
 * <p>See README.md for details on how the annotation processor works and how to
 * use it.</p>
 *
 * <p>Created by mwei on 11/9/16.</p>
 */
@SupportedAnnotationTypes("org.corfudb.annotations.*")
public class ObjectAnnotationProcessor extends AbstractProcessor {

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
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        elementUtils = processingEnv.getElementUtils();
        filer = processingEnv.getFiler();
        messager = processingEnv.getMessager();
    }

    /**
     * {@inheritDoc}
     *
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
        if (classElement.getAnnotation(CorfuObject.class) != null) {

            try {
                generateProxy(classElement);
            } catch (IOException e) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Failed to process class "
                        + classElement.getSimpleName() + " with IOException");
            }
        }
        // deal with any inner classes
        classElement.getEnclosedElements().stream()
                .filter(x -> x instanceof TypeElement)
                .map(x -> (TypeElement) x)
                .forEach(this::processClass);
    }

    /** Return a SMR function name, extracting it from the annotations if available.
     *
     * @param smrMethod     The ExecutableElement to extract the name from.
     * @return              An SMR Function name string.
     */
    public String getSmrFunctionName(ExecutableElement smrMethod) {
        MutatorAccessor mutatorAccessor = smrMethod.getAnnotation(MutatorAccessor.class);
        Mutator mutator = smrMethod.getAnnotation(Mutator.class);
        return  (mutator != null && !mutator.name().equals("")) ? mutator.name() :
                (mutatorAccessor != null && !mutatorAccessor.name().equals(""))
                        ?
                mutatorAccessor.name() : smrMethod.getSimpleName().toString();
    }

    /** Class to hold data about SMR Methods. */
    class SmrMethodInfo {
        /** The method itself. */
        final ExecutableElement method;
        /** The interface, if present, which provided the element. */
        final TypeElement interfaceOverride;
        /** If the element should be excluded from instrumentation. */
        final boolean doNotAdd;
        /** If the element has conflictParameter annotations. */
        final boolean hasConflictAnnotations;
        /** If the element has a function to calculate conflict params. */
        final String conflictFunction;

        public SmrMethodInfo(ExecutableElement method,
                             TypeElement interfaceOverride) {
            this(method, interfaceOverride, false);
        }

        public SmrMethodInfo(ExecutableElement method,
                             TypeElement interfaceOverride,
                             boolean doNotAdd) {
            this.method = method;
            this.interfaceOverride = interfaceOverride;
            this.doNotAdd = doNotAdd;
            this.hasConflictAnnotations = checkForConflictAnnotations(method);


            Accessor accessor = method.getAnnotation(Accessor.class);
            MutatorAccessor mutatorAccessor = method.getAnnotation(MutatorAccessor.class);
            Mutator mutator = method.getAnnotation(Mutator.class);

            // Check if there is a conflictFunction
            if (accessor != null && !accessor.conflictParameterFunction().equals("")) {
                conflictFunction = accessor.conflictParameterFunction();
            } else if (mutator != null && !mutator.conflictParameterFunction().equals("")) {
                conflictFunction = mutator.conflictParameterFunction();
            } else if (mutatorAccessor != null && !mutatorAccessor.conflictParameterFunction()
                    .equals("")) {
                conflictFunction = mutatorAccessor.conflictParameterFunction();
            } else {
                conflictFunction = null;
            }

            if (conflictFunction != null && hasConflictAnnotations) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Method "
                        + method.getSimpleName()
                        + " cannot have both conflict annotations and conflict function '"
                        + conflictFunction + "'");

            }
        }

        /** Return whether the given method has conflict annotations.
         * @param method    The method to check for.
         * @return          True, if conflict annotations are present.
         */
        private boolean checkForConflictAnnotations(ExecutableElement method) {
            return method.getParameters().stream()
                    .anyMatch(x -> x.getAnnotation(ConflictParameter.class)
                                            != null);
        }
    }

    /** Generate a proxy file.
     *
     * @param classElement  The element to generate a proxy file for
     * @throws IOException  If we could not generate the proxy file
     */
    public void generateProxy(TypeElement classElement)
            throws IOException {
        // Extract the package name for the class. We'll need this to generate the proxy file.
        String packageName = elementUtils.getPackageOf(classElement).toString();
        // Calculate the name of the proxy, which appends $CORFUSMR to the class name.
        ClassName proxyName = classElement.getNestingKind() == NestingKind.TOP_LEVEL
                ?
                // Top level classes can just use the name
                ClassName.bestGuess(classElement.getSimpleName() + ICorfuSMR.CORFUSMR_SUFFIX) :
                // Otherwise we need to include the enclosing element as well.
                ClassName.bestGuess(classElement.getEnclosingElement().getSimpleName()
                        + "$" + classElement.getSimpleName() + ICorfuSMR.CORFUSMR_SUFFIX);
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

        // But we also will want a wrapper for every public constructor as well.
        classElement.getEnclosedElements().stream()
                .filter(x -> x.getKind() == ElementKind.CONSTRUCTOR)
                .map(x -> (ExecutableElement) x)
                .forEach(x -> {
                    typeSpecBuilder.addMethod(MethodSpec.constructorBuilder()
                            .addModifiers(Modifier.PUBLIC)
                            .addParameters(x.getParameters().stream()
                                                .map(param -> ParameterSpec.builder(
                                                        TypeName.get(param.asType()),
                                                        param.getSimpleName().toString()
                                                ).build())
                                            .collect(Collectors.toSet())
                            )
                            .addStatement("super($L)",
                                    x.getParameters().stream()
                                            .map(param -> param.getSimpleName().toString())
                                            .collect(Collectors.joining(", "))
                                    )
                            .build()
                    );
                });

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
        final Set<SmrMethodInfo> methodSet = classElement.getEnclosedElements().stream()
                .filter(x -> x.getKind() == ElementKind.METHOD)
                .map(x -> (ExecutableElement) x)
                .filter(x -> !x.getModifiers().contains(Modifier.STATIC)) // don't want static
                .map(x -> new SmrMethodInfo(x, null))
                .collect(Collectors.toCollection(HashSet::new));

        // Deal with the inheritance tree for this class.
        TypeElement superClassElement = classElement;

        do {
            // Get the next superClass.
            superClassElement = ((TypeElement)((DeclaredType)superClassElement.getSuperclass())
                    .asElement());
            boolean samePackage = ClassName.get(superClassElement).packageName().equals(
                                        ClassName.get(classElement).packageName());

            superClassElement.getEnclosedElements().stream()
                    .filter(x -> x.getKind() == ElementKind.METHOD)
                    .map(x -> (ExecutableElement) x)
                    .filter(x -> !x.getModifiers().contains(Modifier.FINAL)) // Can't override final
                    .filter(x -> !x.getModifiers().contains(Modifier.STATIC)) // or static...
                    //or protected, when we're not in the same package.
                    .filter(x -> samePackage || !x.getModifiers().contains(Modifier.PROTECTED))
                    //only public, or protected from same package filtered above
                    .filter(x -> x.getModifiers().contains(Modifier.PUBLIC)
                            ||
                            x.getModifiers().contains(Modifier.PROTECTED))
                    .forEach(x -> {
                        if (methodSet.stream().noneMatch(y -> {
                            // If this method is present in the parent, we need to check
                            // the parameters
                            if (y.method.getSimpleName().equals(x.getSimpleName())) {
                                if (y.method.getParameters().size() == x.getParameters().size()) {
                                    // If there are generics, assume a match for now
                                    // TODO: Properly handle generics
                                    if (x.getParameters().stream().anyMatch(p ->
                                        p.asType().getKind() == TypeKind.TYPEVAR)) {
                                        return true;
                                    } else {
                                        // Otherwise the method name will match
                                        return x.toString().equals(y.method.toString());
                                    }
                                }
                            }
                            return false;
                        })) {
                            methodSet.add(new SmrMethodInfo(x, null));
                        }
                    });

                    // Terminate if the current superClass is java.lang.Object.
        } while (!ClassName.get(superClassElement).equals(ClassName.OBJECT));

        // Deal with interfaces.
        List<TypeMirror> allInterfaces = new ArrayList<>();
        List<TypeMirror> visitedInterfaces = new ArrayList<>();

        allInterfaces.addAll(classElement.getInterfaces());

        while (allInterfaces.stream()
                .filter(x -> !visitedInterfaces.contains(x))
                .map(x -> (TypeElement) ((DeclaredType)x).asElement())
                .filter(x -> x.getInterfaces().size() > 0)
                .count() > 0) {
            List<TypeMirror> interfaceTypes = new ArrayList<>();
            allInterfaces.stream()
                    .filter(x -> !visitedInterfaces.contains(x))
                    .filter(x -> ((TypeElement) ((DeclaredType)x).asElement())
                            .getInterfaces().size() > 0)
                    .forEach(x -> {
                        TypeElement t = (TypeElement) ((DeclaredType)x).asElement();
                        interfaceTypes.addAll(t.getInterfaces());
                        visitedInterfaces.add(x);
                    });
            allInterfaces.addAll(interfaceTypes);
        }

        Set<TypeName> interfacesToAdd = new HashSet<>();

        allInterfaces.forEach(iface -> {
            TypeElement ifaceElement = (TypeElement) ((DeclaredType)iface).asElement();
            // need to traverse the interface inheritance tree.
            ifaceElement.getEnclosedElements().stream()
                    .filter(x -> x.getKind() == ElementKind.METHOD)
                    .map(x -> (ExecutableElement) x)
                    .forEach(method -> {
                        Optional<SmrMethodInfo> overwrite =
                                methodSet.stream()
                                        .filter(x -> method.toString().equals(x.method.toString()))
                                        .findFirst();
                        if (method.getAnnotation(Accessor.class) != null
                                || method.getAnnotation(Mutator.class) != null
                                || method.getAnnotation(MutatorAccessor.class) != null
                                || method.getAnnotation(PassThrough.class) != null) {
                            if (overwrite.isPresent()) {
                                methodSet.remove(overwrite.get());
                                methodSet.add(new SmrMethodInfo(method, null));
                            } else {
                                methodSet.add(new SmrMethodInfo(method, ifaceElement));
                            }
                        } else if (method.getAnnotation(InterfaceOverride.class) != null) {
                            interfacesToAdd.add(ParameterizedTypeName.get(ifaceElement.asType()));
                            if (overwrite.isPresent()) {
                                methodSet.remove(overwrite.get());
                            }
                            methodSet.add(new SmrMethodInfo(method,
                                        ifaceElement));
                        } else if (
                                // if we have a implementation, and we aren't already
                                // implemented by the object.
                                method.getModifiers().contains(Modifier.DEFAULT)
                                        &&
                                methodSet.stream().noneMatch(x -> x.method.toString()
                                .equals(method.toString()))) {
                            methodSet.add(new SmrMethodInfo(method,
                                            ifaceElement, true));
                        }
                    });
        });

        // remove any methods which end with $CORFUSMR
        methodSet.removeIf(x -> x.method.getSimpleName()
                .toString().endsWith(ICorfuSMR.CORFUSMR_SUFFIX));

        // Verify that all methods that require an up call have specified as annotated name
        checkAnnotatedNames(methodSet);

        // Gather methods that require upcalls
        Set<SmrMethodInfo> upCalls = methodSet.stream()
                .filter(x -> ((x.method.getAnnotation(Mutator.class) != null)
                        && !x.method.getAnnotation(Mutator.class).noUpcall())
                        || ((x.method.getAnnotation(MutatorAccessor.class) != null)
                        && !x.method.getAnnotation(MutatorAccessor.class).noUpcall()))
                .collect(Collectors.toCollection(HashSet::new));

        checkOverloadConflicts(upCalls);

        // Gather methods that reference upcall methods (i.e. mutators that require no upcalls)
        Set<SmrMethodInfo> noUpcalls = methodSet.stream()
                .filter(x -> ((x.method.getAnnotation(Mutator.class) != null)
                        && x.method.getAnnotation(Mutator.class).noUpcall())
                        || ((x.method.getAnnotation(MutatorAccessor.class) != null)
                        && x.method.getAnnotation(MutatorAccessor.class).noUpcall()))
                .collect(Collectors.toCollection(HashSet::new));

        verifyNoUpCallReference(noUpcalls, upCalls);

        // Generate wrapper classes.
        methodSet.stream()
                .filter(x -> !x.doNotAdd)
                .forEach(m -> {

                    ExecutableElement smrMethod = m.method;

                    // Extract each annotation
                    final Accessor accessor = smrMethod.getAnnotation(Accessor.class);
                    final MutatorAccessor mutatorAccessor = smrMethod
                            .getAnnotation(MutatorAccessor.class);
                    final Mutator mutator = smrMethod.getAnnotation(Mutator.class);
                    final TransactionalMethod transactional = smrMethod
                            .getAnnotation(TransactionalMethod.class);

                    // Override the method we will proxy.
                    MethodSpec.Builder ms = MethodSpec.overriding(smrMethod);

                    // This is a hack, but necessary since the modifier list is immutable
                    // and we need to remove the "native" and "default" modifiers.
                    try {
                        Field f = ms.getClass().getDeclaredField("modifiers");
                        f.setAccessible(true);
                        ((List<Modifier>)f.get(ms)).remove(Modifier.NATIVE);
                        ((List<Modifier>)f.get(ms)).remove(Modifier.DEFAULT);
                    } catch (Exception e) {
                        messager.printMessage(Diagnostic.Kind.ERROR,
                                "error trying to change methodspec"
                                + e.getMessage());
                    }

                    // If there is conflict information, calculate the conflict set
                    boolean hasConflictData = false;
                    final String conflictField = "conflictField" + CORFUSMR_FIELD;


                    if (m.conflictFunction != null) {
                        hasConflictData = true;
                        addConflictFieldFromFunctionToMethod(ms, conflictField,
                                m.conflictFunction, smrMethod);
                    } else if (m.hasConflictAnnotations) {
                        hasConflictData = true;
                        addConflictFieldToMethod(ms, conflictField, smrMethod);
                    }

                    // If a mutator, then log the update.
                    if (mutator != null || mutatorAccessor != null) {
                        ms.addStatement(
                                (mutatorAccessor != null ? "long address"
                                        + CORFUSMR_FIELD + " = " : "")
                                        + "proxy" + CORFUSMR_FIELD + ".logUpdate($S,$L,$L$L$L)",
                                getSmrFunctionName(smrMethod),
                                // Don't need upcall result for mutators
                                mutator != null ? "false" :
                                // or mutatorAccessors which return void.
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                        ? "false" : "true",
                                hasConflictData ? conflictField : "null",
                                smrMethod.getParameters().size() > 0 ? "," : "",
                                smrMethod.getParameters().stream()
                                    .map(VariableElement::getSimpleName)
                                    .collect(Collectors.joining(", ")));
                    }


                    // If an accessor (or not annotated), return the object
                    // by doing the underlying call.
                    if (mutatorAccessor != null) {
                        // If the return to the mutatorAccessor is void, we don't need
                        // to do anything...
                        if (!smrMethod.getReturnType().getKind().equals(TypeKind.VOID)) {
                            ms.addStatement("return ("
                                    + ParameterizedTypeName.get(smrMethod.getReturnType())
                                    + ") proxy" + CORFUSMR_FIELD
                                    + ".getUpcallResult(address"
                                    + CORFUSMR_FIELD
                                    +  ", "
                                    + (m.hasConflictAnnotations ? conflictField : "null")
                                    + ")");
                        }
                    } else if (transactional != null) {
                        // If transactional, begin the transaction
                        ms.addCode(smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                ? "proxy" + CORFUSMR_FIELD + ".TXExecute(() -> {" :
                                "return proxy" + CORFUSMR_FIELD + ".TXExecute(() -> {"
                        );
                        ms.addStatement("$Lsuper.$L($L)",
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                        ? "" : "return ",
                                smrMethod.getSimpleName(),
                                smrMethod.getParameters().stream()
                                        .map(VariableElement::getSimpleName)
                                        .collect(Collectors.joining(", ")));
                        ms.addCode(smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                ? "return null; });" : "});"
                        );
                    } else if (smrMethod.getAnnotation(PassThrough.class) != null) {
                        ms.addStatement("$L super.$L($L)",
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                        ? "" : "return ",
                                smrMethod.getSimpleName(),
                                smrMethod.getParameters().stream()
                                        .map(VariableElement::getSimpleName)
                                        .collect(Collectors.joining(", "))
                        );
                    } else if (smrMethod.getAnnotation(InterfaceOverride.class) != null) {
                        ms.addStatement("$L$T.super.$L($L)",
                                    smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                            ? "" : "return ",
                                    ClassName.get(m.interfaceOverride),
                                    smrMethod.getSimpleName(),
                                    smrMethod.getParameters().stream()
                                            .map(VariableElement::getSimpleName)
                                            .collect(Collectors.joining(", ")));
                    } else if (mutator == null) {
                        // Otherwise, just force the access to access the underlying call.
                        ms.addStatement((smrMethod.getReturnType().getKind()
                                        .equals(TypeKind.VOID) ? "" : "return ") + "proxy"
                                        + CORFUSMR_FIELD + ".access(" + "o" + CORFUSMR_FIELD
                                        + " -> {$Lo" + CORFUSMR_FIELD + ".$L($L);$L}," + "$L)",
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                        ? "" : "return ",
                                smrMethod.getSimpleName(),
                                smrMethod.getParameters().stream()
                                        .map(VariableElement::getSimpleName)
                                        .collect(Collectors.joining(", ")),
                                smrMethod.getReturnType().getKind().equals(TypeKind.VOID)
                                        ? "return null;" : "",
                                (m.hasConflictAnnotations ? conflictField : "null")
                        );
                    }
                    // Don't instrument methods not marked for instrumentation
                    if (smrMethod.getAnnotation(DontInstrument.class) == null) {
                        typeSpecBuilder.addMethod(ms.build());
                    }
                });

        addUpcallMap(typeSpecBuilder, originalName, interfacesToAdd, methodSet);
        addUndoRecordMap(typeSpecBuilder, originalName, interfacesToAdd, methodSet);
        addUndoMap(typeSpecBuilder, originalName, interfacesToAdd, methodSet);
        addResetSet(typeSpecBuilder, originalName, interfacesToAdd, methodSet);

        typeSpecBuilder
                .addSuperinterfaces(interfacesToAdd);
        // Mark the object as instrumented, so we don't instrument it again.
        typeSpecBuilder
                .addAnnotation(AnnotationSpec.builder(InstrumentedCorfuObject.class).build());

        JavaFile javaFile = JavaFile
                .builder(packageName,
                        typeSpecBuilder.build())
                .build();

        javaFile.writeTo(filer);
    }

    /*
     * Verify that methods with a mutator annotation specify the name field
     */
    void checkAnnotatedNames(Set<SmrMethodInfo> methodSet) {

        for (SmrMethodInfo smrMethodInfo : methodSet) {
            if ((smrMethodInfo.method.getAnnotation(Mutator.class) != null
                    && smrMethodInfo.method.getAnnotation(Mutator.class).name().isEmpty())
                    || (smrMethodInfo.method.getAnnotation(MutatorAccessor.class) != null
                    && smrMethodInfo.method.getAnnotation(MutatorAccessor.class).name()
                        .isEmpty())) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Method "
                        + smrMethodInfo.method.getSimpleName()
                        + " must specify a name in its annotation name field");
            }
        }
    }

    String getAnnotationNameField(ExecutableElement method) {
        String name = "";

        if (method.getAnnotation(Mutator.class) != null) {
            name = method.getAnnotation(Mutator.class).name();
        } else if (method.getAnnotation(MutatorAccessor.class) != null) {
            name = method.getAnnotation(MutatorAccessor.class).name();
        }

        return name;
    }

    /*
     * Verify that the no upcall methods reference valid upcall methods
     */
    void verifyNoUpCallReference(Set<SmrMethodInfo> noUpcalls, Set<SmrMethodInfo> upCalls) {

        Set<String> upCallMethodNames = upCalls.stream()
                .map(x -> getAnnotationNameField(x.method))
                .collect(Collectors.toCollection(HashSet::new));

        for (SmrMethodInfo smrMethodInfo : noUpcalls) {
            String methodName = getAnnotationNameField(smrMethodInfo.method);

            if (!upCallMethodNames.contains(methodName)) {
                messager.printMessage(Diagnostic.Kind.ERROR,
                        "Error " + smrMethodInfo.method.toString()
                                + " referencing unknown smr method ");
            }
        }
    }

    /** Verify that no two methods have the same annotation name.
     * @param possibleConflictMethods Methods that are mutators and require upcalls
     */
    void checkOverloadConflicts(Set<SmrMethodInfo> possibleConflictMethods) {
        Set<SmrMethodInfo> allMethods = new HashSet<>(possibleConflictMethods);

        for (SmrMethodInfo smrMethodInfo : possibleConflictMethods) {
            Set<SmrMethodInfo> setDiff = new HashSet<>(allMethods);
            setDiff.remove(smrMethodInfo);

            String baseMethodName = getAnnotationNameField(smrMethodInfo.method);

            for (SmrMethodInfo smrMethodInfo2 : setDiff) {
                String methodName = getAnnotationNameField(smrMethodInfo2.method);

                if (baseMethodName.equals(methodName)) {
                    messager.printMessage(Diagnostic.Kind.ERROR,
                            "Error overloading with annotation name "
                                    + smrMethodInfo.method.toString() + " and "
                                    + smrMethodInfo2.method.toString());
                    
                }
            }
        }
    }

    /** Add the reset set and the getter for the set.
     *
     * @param typeSpecBuilder   The typespec builder to add the reset set to
     * @param originalName      The name of the original base type (without $CORFUSMR)
     * @param interfacesToAdd   A list of interfaces to add for instrumentation.
     * @param methodSet         The set of methods to add for instrumentation.
     */
    private void addResetSet(TypeSpec.Builder typeSpecBuilder, TypeName originalName,
                             Set<TypeName> interfacesToAdd, Set<SmrMethodInfo> methodSet) {
        // Generate the initializer for the reset set.
        String resetString = methodSet.stream()
                .filter(x -> x.method.getAnnotation(Mutator.class) != null
                        && x.method.getAnnotation(Mutator.class).reset()
                        || x.method.getAnnotation(MutatorAccessor.class) != null
                                && x.method.getAnnotation(MutatorAccessor.class).reset())
                .map(x -> "\n.add(\"" + getSmrFunctionName(x.method) + "\")")
                .collect(Collectors.joining());

        FieldSpec resetSet = FieldSpec.builder(ParameterizedTypeName.get(ClassName.get(Set.class),
                ClassName.get(String.class)), "resetSet" + CORFUSMR_FIELD,
                Modifier.FINAL, Modifier.PUBLIC)
                .initializer("new $T()$L.build()",
                        ParameterizedTypeName.get(ClassName.get(ImmutableSet.Builder.class),
                                ClassName.get(String.class)), resetString)
                .build();

        typeSpecBuilder.addField(resetSet);
        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuResetSet")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Set.class),
                        ClassName.get(String.class)))
                .addStatement("return $L", "resetSet" + CORFUSMR_FIELD)
                .build());

    }

    private void addUpcallMap(TypeSpec.Builder typeSpecBuilder, TypeName originalName,
                              Set<TypeName> interfacesToAdd, Set<SmrMethodInfo> methodSet) {

        // Generate the upcall string and associated map.
        String upcallString = methodSet.stream()
                .filter(x -> x.method.getAnnotation(MutatorAccessor.class) != null
                        || (x.method.getAnnotation(Mutator.class) != null
                        && !x.method.getAnnotation(Mutator.class).noUpcall()))
                .map(x -> "\n.put(\"" + getSmrFunctionName(x.method) + "\", "
                        + "(obj, args) -> { " + (x.method.getReturnType()
                            .getKind().equals(TypeKind.VOID)
                        ? "" : "return ") + "obj." + x.method.getSimpleName() + "("
                        + IntStream.range(0, x.method.getParameters().size())
                                .mapToObj(i ->
                                        "(" + x.method.getParameters().get(i)
                                                .asType().toString() + ")" + " args[" + i + "]")
                                .collect(Collectors.joining(", "))
                        + ");" + (x.method.getReturnType().getKind().equals(TypeKind.VOID)
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
                                ParameterizedTypeName.get(ClassName
                                                .get(ICorfuSMRUpcallTarget.class),
                                        originalName)), upcallString)
                .build();

        typeSpecBuilder
                .addField(upcallMap);

        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuSMRUpcallMap")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ClassName.get(String.class),
                        ParameterizedTypeName.get(ClassName.get(ICorfuSMRUpcallTarget.class),
                                originalName)))
                .addStatement("return $L", "upcallMap" + CORFUSMR_FIELD)
                .build());
    }

    /** Generate the undo record map for this type. */
    private void addUndoRecordMap(TypeSpec.Builder typeSpecBuilder, TypeName originalName,
                                  Set<TypeName> interfacesToAdd, Set<SmrMethodInfo> methodSet) {
        // And generate a map for the undoRecord and undo functions, if available.
        // We may have to add additional interfaces during this process.

        // Generate the undo string and associated map.
        String undoRecordString = methodSet.stream()
                .filter(x -> x.method.getAnnotation(MutatorAccessor.class) != null
                        || x.method.getAnnotation(Mutator.class) != null)
                .filter(x -> x.method.getAnnotation(MutatorAccessor.class) == null
                        || !x.method.getAnnotation(MutatorAccessor.class)
                            .undoRecordFunction().equals(""))
                .filter(x -> x.method.getAnnotation(Mutator.class) == null
                        || !x.method.getAnnotation(Mutator.class).undoRecordFunction().equals(""))
                .map(x -> {
                    MutatorAccessor mutatorAccessor = x.method.getAnnotation(MutatorAccessor.class);
                    Mutator mutator = x.method.getAnnotation(Mutator.class);
                    String undoRecordFunction = mutator == null
                            ? mutatorAccessor.undoRecordFunction() :
                            mutator.undoRecordFunction();

                    Optional<SmrMethodInfo> mi = methodSet.stream()
                            .filter(y ->
                                    y.method.getSimpleName().toString().equals(undoRecordFunction))
                            .findFirst();

                    // Don't generate a record since we don't have a matching method
                    if (!mi.isPresent()) {
                        messager.printMessage(Diagnostic.Kind.MANDATORY_WARNING,
                                "No undoRecord method found for "
                                + x.method.getSimpleName() + " named " + undoRecordFunction);
                        return "";
                    }

                    // Check that the signature matches what we expect. (1+original)
                    if (mi.get().method.getParameters().size()
                            != x.method.getParameters().size() + 1) {
                        messager.printMessage(Diagnostic.Kind.ERROR, "undoRecord method "
                                + undoRecordFunction + " contained the wrong number of parameters");
                    }

                    // Add the interface, if present.
                    if (mi.get().interfaceOverride != null) {
                        interfacesToAdd.add(ParameterizedTypeName
                                .get(mi.get().interfaceOverride.asType()));
                    }
                    String callingConvention = mi.get().interfaceOverride == null ? "this." :
                            mi.get().interfaceOverride.getSimpleName() + ".super.";

                    return "\n.put(\"" + getSmrFunctionName(x.method) + "\", "
                            + "(obj, args) -> { return "
                            + callingConvention + undoRecordFunction + "(obj,"
                            + IntStream.range(0, x.method.getParameters().size())
                                    .mapToObj(i ->
                                            "(" + mi.get().method.getParameters().get(i + 1)
                                                    .asType().toString() + ")"
                                                    + " args[" + i + "]")
                                    .collect(Collectors.joining(", "))
                            + ");" + "})"; })
                .collect(Collectors.joining());

        FieldSpec undoRecordMap = FieldSpec.builder(ParameterizedTypeName
                        .get(ClassName.get(Map.class),
                ClassName.get(String.class),
                ParameterizedTypeName.get(ClassName.get(IUndoRecordFunction.class),
                        originalName)), "undoRecordMap" + CORFUSMR_FIELD,
                Modifier.PUBLIC, Modifier.FINAL)
                .initializer("new $T()$L.build()",
                        ParameterizedTypeName.get(ClassName.get(ImmutableMap.Builder.class),
                                ClassName.get(String.class),
                                ParameterizedTypeName.get(ClassName.get(IUndoRecordFunction.class),
                                        originalName)), undoRecordString)
                .build();

        typeSpecBuilder.addField(undoRecordMap);

        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuUndoRecordMap")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ClassName.get(String.class),
                        ParameterizedTypeName.get(ClassName.get(IUndoRecordFunction.class),
                                originalName)))
                .addStatement("return $L", "undoRecordMap" + CORFUSMR_FIELD)
                .build());

    }

    /** Generate the undo string and associated map for the type. */
    private void addUndoMap(TypeSpec.Builder typeSpecBuilder, TypeName originalName,
                       Set<TypeName> interfacesToAdd, Set<SmrMethodInfo> methodSet) {
        String undoString = methodSet.stream()
                .filter(x -> x.method.getAnnotation(MutatorAccessor.class) != null
                        || x.method.getAnnotation(Mutator.class) != null)
                .filter(x -> x.method.getAnnotation(MutatorAccessor.class) == null
                        || !x.method.getAnnotation(MutatorAccessor.class)
                            .undoRecordFunction().equals(""))
                .filter(x -> x.method.getAnnotation(Mutator.class) == null
                        || !x.method.getAnnotation(Mutator.class).undoRecordFunction().equals(""))
                .map(x -> {
                    MutatorAccessor mutatorAccessor = x.method.getAnnotation(MutatorAccessor.class);
                    Mutator mutator = x.method.getAnnotation(Mutator.class);
                    String undoFunction = mutator == null ? mutatorAccessor.undoFunction() :
                            mutator.undoFunction();

                    Optional<SmrMethodInfo> mi = methodSet.stream()
                            .filter(y ->
                                    y.method.getSimpleName().toString().equals(undoFunction))
                            .findFirst();

                    // Don't generate a record since we don't have a matching method
                    if (!mi.isPresent()) {
                        messager.printMessage(Diagnostic.Kind.MANDATORY_WARNING,
                                "No undo method found for " + x.method.getSimpleName()
                                        + " named " + undoFunction);
                        return "";
                    }

                    // Check that the signature matches what we expect. (2+original)
                    if (mi.get().method.getParameters().size()
                            != x.method.getParameters().size() + 2) {
                        messager.printMessage(Diagnostic.Kind.ERROR, "undoRecord method "
                                + undoFunction + " contained the wrong number of parameters");
                    }

                    // Add the interface, if present.
                    if (mi.get().interfaceOverride != null) {
                        interfacesToAdd.add(ParameterizedTypeName
                                .get(mi.get().interfaceOverride.asType()));
                    }
                    String callingConvention = mi.get().interfaceOverride == null ? "this." :
                            mi.get().interfaceOverride.getSimpleName() + ".super.";

                    return "\n.put(\"" + getSmrFunctionName(x.method) + "\", "
                            + "(obj, undoRecord, args) -> {" + callingConvention
                            + undoFunction + "(obj, ("
                            + ParameterizedTypeName.get(mi.get().method.getParameters()
                            .get(1).asType())
                            + ") undoRecord, "
                            + IntStream.range(0, x.method.getParameters().size())
                                    .mapToObj(i ->
                                            "(" + mi.get().method.getParameters().get(i + 2)
                                                    .asType().toString() + ")"
                                                    + " args[" + i + "]")
                                    .collect(Collectors.joining(", "))
                            + ");})";
                })
                .collect(Collectors.joining());


        FieldSpec undoMap = FieldSpec.builder(ParameterizedTypeName.get(ClassName.get(Map.class),
                ClassName.get(String.class),
                ParameterizedTypeName.get(ClassName.get(IUndoFunction.class),
                        originalName)), "undoMap" + CORFUSMR_FIELD,
                Modifier.PUBLIC, Modifier.FINAL)
                .initializer("new $T()$L.build()",
                        ParameterizedTypeName.get(ClassName.get(ImmutableMap.Builder.class),
                                ClassName.get(String.class),
                                ParameterizedTypeName.get(ClassName.get(IUndoFunction.class),
                                        originalName)), undoString)
                .build();

        typeSpecBuilder.addField(undoMap);

        typeSpecBuilder.addMethod(MethodSpec.methodBuilder("getCorfuUndoMap")
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ClassName.get(String.class),
                        ParameterizedTypeName.get(ClassName.get(IUndoFunction.class),
                                originalName)))
                .addStatement("return $L", "undoMap" + CORFUSMR_FIELD)
                .build());

    }

    /** Add a conflict field to the method.
     *
     * @param ms                The builder to add the field to.
     * @param conflictField     The name of the field to add
     * @param smrMethod         The method element.
     */
    public void addConflictFieldToMethod(MethodSpec.Builder ms, String conflictField,
                                         ExecutableElement smrMethod) {
        ms.addStatement("$T $L = new Object[]{$L}", Object[].class, conflictField,
                smrMethod.getParameters()
                        .stream()
                        .filter(y -> y.getAnnotation(ConflictParameter.class)
                                != null)
                        .map(y -> y.getSimpleName())
                        .collect(Collectors.joining(", ")));
    }

    /** Add a conflict field to the method.
     *
     * @param ms                The builder to add the field to.
     * @param conflictField     The name of the field to add
     * @param functionName      The function to call to obtain the parameters.
     * @param smrMethod         The method element.
     */
    public void addConflictFieldFromFunctionToMethod(MethodSpec.Builder ms,
                                                     String conflictField, String functionName,
                                                     ExecutableElement smrMethod) {
        ms.addStatement("$T $L = $L($L)", Object[].class, conflictField, functionName,
                smrMethod.getParameters()
                        .stream()
                        .map(y -> y.getSimpleName())
                        .collect(Collectors.joining(", ")));
    }
}
