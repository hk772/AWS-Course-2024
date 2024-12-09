package org.example.other;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MsgJsonizer {
    public static Message deJasonize(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Message message = objectMapper.readValue(json, Message.class);
        return message;
    }

    public static String jsonize(Message message) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String messageJson = objectMapper.writeValueAsString(message);
        return messageJson;
    }
}
