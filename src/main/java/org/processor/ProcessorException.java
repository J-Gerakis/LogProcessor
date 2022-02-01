package org.processor;

public class ProcessorException extends Exception {
    public ProcessorException(String message, Throwable originError){
        super(message, originError);
    }

    public ProcessorException(String message){
        super(message);
    }
}
