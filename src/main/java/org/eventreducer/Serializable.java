package org.eventreducer;

import lombok.SneakyThrows;

public class Serializable {

    private Serializer serializer;

    public <T extends Serializable> Serializer<T> entitySerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (serializer != null) {
            return serializer;
        }
        String className = this.getClass().getPackage().getName() + "." + this.getClass().getSimpleName() + "Serializer";
        serializer = (Serializer) Class.forName(className).newInstance();
        return serializer;
    }

    @Override @SneakyThrows
    public String toString() {
        return entitySerializer().toString(this);
    }
}
