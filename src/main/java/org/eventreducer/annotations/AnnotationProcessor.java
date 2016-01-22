package org.eventreducer.annotations;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.service.AutoService;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.googlecode.cqengine.IndexedCollection;
import com.squareup.javapoet.*;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;
import org.eventreducer.IndexFactory;
import org.javatuples.Pair;

import javax.annotation.processing.Processor;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link org.eventreducer.annotations.Property} annotation processor
 *
 * The idea behind this is to generate efficient serializer and deserializer
 * for property holders (commands and events) compile time.
 *
 * It will sort property field names lexicographically (using {@link String#compareTo(String)})
 * to ensure deterministic serialization layout and will compute <code>HASH160 (RIPEMD160(SHA256(x))</code> over
 * every field to produce a unique layout identifier.
 */
@AutoService(Processor.class)
public class AnnotationProcessor extends BasicAnnotationProcessor {
    @Override
    protected Iterable<? extends ProcessingStep> initSteps() {
        return Collections.singletonList(new Step());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }



    class Step implements ProcessingStep {

        private final Map<TypeElement, List<Element>> properties = new HashMap<>();
        private final Map<TypeElement, List<Pair<Annotation, Element>>> indices = new HashMap<>();

        @Override
        public Set<? extends Class<? extends Annotation>> annotations() {
            return ImmutableSet.of(Property.class, Index.class, Serializable.class);
        }

        @Override
        public Set<Element> process(SetMultimap<Class<? extends Annotation>, Element> elementsByAnnotation) {

            extractPropertiesAndIndicesByClass(elementsByAnnotation);
            sortLexicographically();
            generateSerializer();

            return ImmutableSet.of();
        }

        private void generateSerializer() {
            properties.forEach((enclosing, elements) -> {
                try {
                    byte[] ripemdHash = computeLayoutHash(elements);

                    FieldSpec hashField = FieldSpec.builder(byte[].class, "hash", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC).
                            initializer("$T.getDecoder().decode($S)", Base64.class, Base64.getEncoder().encodeToString(ripemdHash)).
                            build();

                    MethodSpec.Builder toString = MethodSpec.methodBuilder("toString").
                            addModifiers(Modifier.PUBLIC).
                            addParameter(ClassName.get(enclosing), "o").
                            returns(String.class).
                            addCode("String output = $S;", "<# " + enclosing.getSimpleName().toString());

                    for (Element element : elements) {
                        toString.addCode("output += \" \" + $S + \"=\" + o.$L;", element.getSimpleName().toString(), element.getSimpleName().toString());
                    }

                    toString.addCode("output += \">\"; return output;");


                    MethodSpec hash = MethodSpec.methodBuilder("hash").
                    addModifiers(Modifier.PUBLIC).
                    returns(byte[].class).
                    addCode("return this.hash;").build();

                    MethodSpec getIndex = MethodSpec.methodBuilder("getIndex").
                            addModifiers(Modifier.PUBLIC).
                            addParameter(IndexFactory.class, "indexFactory").
                            returns(ParameterizedTypeName.get(ClassName.get(IndexedCollection.class), ClassName.get(enclosing))).
                            addCode("return indexFactory.getIndexedCollection($T.class);", ClassName.get(enclosing)).
                            build();

                    MethodSpec index = MethodSpec.methodBuilder("index").
                            addParameter(IndexFactory.class, "indexFactory").
                            addParameter(ClassName.get(enclosing), "o").
                            addModifiers(Modifier.PUBLIC).
                            addCode("$T index = getIndex(indexFactory);index.add(o);", ParameterizedTypeName.get(ClassName.get(IndexedCollection.class), ClassName.get(enclosing))).build();

                    MethodSpec.Builder configureIndicesBuilder = MethodSpec.methodBuilder("configureIndices").
                            addParameter(IndexFactory.class, "indexFactory").
                            addModifiers(Modifier.PUBLIC).
                            addException(IndexFactory.IndexNotSupported.class).
                            addCode("$T index = getIndex(indexFactory);", ParameterizedTypeName.get(ClassName.get(IndexedCollection.class), ClassName.get(enclosing)));


                    if (indices.get(enclosing) != null) {
                        indices.get(enclosing).stream().forEachOrdered(pair -> {
                            Index annotation = (Index) pair.getValue0();
                            Element el = pair.getValue1();
                            String features = Joiner.on(", ").
                                    join(Arrays.asList(annotation.features()).stream().
                                            map(indexFeature -> CodeBlock.builder().add("$T.$L", IndexFactory.IndexFeature.class, indexFeature.toString()).build().toString()).
                                            collect(Collectors.toList()));

                            configureIndicesBuilder.addCode("index.addIndex(indexFactory.getIndexOnAttribute($T.$L, $L));",
                                    ClassName.get(enclosing), el.getSimpleName().toString(), features);
                        });
                    }

                    String serializerClassName = enclosing.getSimpleName().toString() + "Serializer";
                    TypeSpec serializer = TypeSpec.classBuilder(serializerClassName).
                            addModifiers(Modifier.PUBLIC, Modifier.FINAL).
                            addAnnotation(AnnotationSpec.builder(Serializer.class).addMember("value", "$L.class", enclosing.getQualifiedName()).build()).
                            superclass(ParameterizedTypeName.get(ClassName.get(org.eventreducer.Serializer.class), ClassName.get(enclosing))).
                            addField(hashField).
                            addMethod(toString.build()).
                            addMethod(hash).
                            addMethod(getIndex).
                            addMethod(index).
                            addMethod(configureIndicesBuilder.build()).
                            build();

                    JavaFile klass = JavaFile.builder(processingEnv.getElementUtils().getPackageOf(enclosing).toString(), serializer).build();

                    try {
                        klass.writeTo(processingEnv.getFiler());
                    } catch (IOException e) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), enclosing);
                    }
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), enclosing);
                }
            });
        }

        private byte[] typeReference(TypeMirror t) throws UnsupportedPropertyType {
            switch (t.getKind()) {
                case BYTE:
                    return new byte[]{0};
                case SHORT:
                    return new byte[]{1};
                case INT:
                    return new byte[]{2};
                case LONG:
                    return new byte[]{3};
                case FLOAT:
                    return new byte[]{4};
                case DOUBLE:
                    return new byte[]{5};
                case BOOLEAN:
                    return new byte[]{6};
                case CHAR:
                    return new byte[]{7};
                case DECLARED:
                    TypeElement typeElement = (TypeElement) processingEnv.getTypeUtils().asElement(t);
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Byte")) {
                        return new byte[]{0};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Short")) {
                        return new byte[]{1};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Integer")) {
                        return new byte[]{2};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Long")) {
                        return new byte[]{3};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Float")) {
                        return new byte[]{4};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Double")) {
                        return new byte[]{5};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Boolean")) {
                        return new byte[]{6};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.Character")) {
                        return new byte[]{7};
                    }

                    if (typeElement.getKind() == ElementKind.ENUM) {
                        return new byte[]{(byte) 254};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.lang.String")) {
                        return new byte[]{8};
                    }
                    if (typeElement.getQualifiedName().contentEquals("java.util.UUID")) {
                        return new byte[]{9};
                    }
                    throw new UnsupportedPropertyType(typeElement.getQualifiedName().toString());
                case ARRAY:
                    ArrayType array = (ArrayType) t;
                    return Bytes.concat(new byte[]{(byte)255}, typeReference(array.getComponentType()));
                default:
                    throw new UnsupportedPropertyType(t.toString());
            }
        }

        private byte[] computeLayoutHash(List<Element> elements) {
            Hasher hasher = Hashing.sha256().newHasher();
            elements.stream().forEachOrdered(element -> {
                hasher.putString(element.getSimpleName(), Charsets.UTF_8);
                try {
                    hasher.putBytes(typeReference(element.asType()));
                } catch (UnsupportedPropertyType e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
                }
            });

            RIPEMD160Digest ripemd160 = new RIPEMD160Digest();
            byte[] shaHash = hasher.hash().asBytes();
            byte[] ripemdHash = new byte[ripemd160.getDigestSize()];
            ripemd160.update(shaHash, 0, shaHash.length);
            ripemd160.doFinal(ripemdHash, 0);
            return ripemdHash;
        }

        private void sortLexicographically() {
            properties.forEach((enclosing, elements) ->
                    properties.put(enclosing, elements.stream().
                    sorted((x, y) -> x.getSimpleName().toString().compareTo(y.getSimpleName().toString())).collect(Collectors.toList())));
        }

        private void extractPropertiesAndIndicesByClass(SetMultimap<Class<? extends Annotation>, Element> elementsByAnnotation) {
            elementsByAnnotation.entries().stream().forEach(entry -> {
                if (entry.getKey() == Property.class) {
                    Element element = entry.getValue();
                    if (element.getModifiers().contains(Modifier.PRIVATE) || element.getModifiers().contains(Modifier.PROTECTED)) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "@Property can't be applied to private or protected fields", element);
                    }

                    Element enclosingElement = element.getEnclosingElement();
                    if (enclosingElement.getKind() != ElementKind.CLASS) {
                        throw new InternalError("@Property has to be set on a field enclosed in a class");
                    }
                    TypeElement enclosingClass = (TypeElement) enclosingElement;

                    if (!properties.containsKey(enclosingClass)) {
                        properties.put(enclosingClass, new LinkedList<>());
                    }
                    properties.get(enclosingClass).add(element);
                }
                if (entry.getKey() == Index.class) {
                    Element element = entry.getValue();
                    if (element.getModifiers().contains(Modifier.PRIVATE) || element.getModifiers().contains(Modifier.PROTECTED)) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "@Index can't be applied to private or protected fields", element);
                    }

                    if (!element.getModifiers().contains(Modifier.STATIC)) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "@Index can't be applied to non-static fields", element);
                    }

                    Element enclosingElement = element.getEnclosingElement();
                    if (enclosingElement.getKind() != ElementKind.CLASS) {
                        throw new InternalError("@Index has to be set on a field enclosed in a class");
                    }
                    TypeElement enclosingClass = (TypeElement) enclosingElement;

                    if (!indices.containsKey(enclosingClass)) {
                        indices.put(enclosingClass, new LinkedList<>());
                    }
                    indices.get(enclosingClass).add(Pair.with(element.getAnnotation(Index.class), element));
                }
                if (entry.getKey() == Serializable.class) {
                    TypeElement enclosingClass = (TypeElement) entry.getValue();
                    if (!properties.containsKey(enclosingClass)) {
                        properties.put(enclosingClass, new LinkedList<>());
                    }
                }
            });
        }
    }

    private class UnsupportedPropertyType extends Exception {
        private final String qualifiedName;

        public UnsupportedPropertyType(String qualifiedName) {
            this.qualifiedName = qualifiedName;
        }

        @Override
        public String getMessage() {
            return "Unsupported property type " + qualifiedName;
        }
    }
}
