# Corfu Annotation Processor

## Purpose

The Corfu annotation processor pre-processes Java classes so
that they can be used by the Corfu runtime. The annotation
processor generates a wrapper class, which redirects calls
to the Corfu runtime. If a wrapper class is not available,
the Corfu runtime must use runtime instrumentation, which
is much slower.

## How to use

The Corfu annotation processor advertises itself automatically,
With most modern versions of Maven or Gradle, any
application which depends on the runtime should automatically
invoke the Corfu annotation processor during compile.

The annotation processor only processes classes which are 
annotated with the @CorfuObject annotation.

## How it works

The annotation processor searches for classes which are
annotated with the @CorfuObject annotation. When such a
class is encountered, the annotation processor generates
a new class with the $CORFUSMR class suffix. For example,
SMRMap becomes SMRMap$CORFUSMR. This is known as the "proxy"
class. The annotation processor must generate a new class 
because without hacks (such as the ones lombok exploits), 
annotation processors are not capable of modifying existing 
classes.

Proxy classes are a shell for the real class. When the user
opens a Corfu object, the runtime generates a new instance
of the proxy class instead. The proxy class contains a
ICorfuSMRProxy, which generates copies of the original object
and applies updates from the Corfu log.

The proxy class generated extends the original class, so
that all valid method calls on the original class are 
valid on the proxy. The annotation processor searches
the entire hierarchy of the original class for any method
annotated with the @Accessor, @MutatorAccessor and @Mutator
methods. For each one of those methods, the proxy class
overrides the implementation of the original class and calls
the ICorfuSMRProxy instead, using the access and logSMRUpdate
methods.

The annotation processor generates three maps which are used
by the proxy: the upcall map, the undoRecord map and the undo
map. These maps allow the ICorfuSMRProxy to apply or undo
SMRUpdate records by using a name, instead of relying on 
reflection.

An @TransactionalMethod annotation tells the runtime to execute
a method as a transaction, while an @PassThrough call directly
calls the method implementation without any redirection to 
the runtime (via super).