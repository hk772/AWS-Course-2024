package org.example.Messages;

public class Message {
    public String localID;
    public String content;

    public Message(String id, String content) {
        this.localID = id;
        this.content = content;
    }

    public Message() {
    }

    public String getLocalID() {
        return localID;
    }

    public void setLocalID(String localID) {
        this.localID = localID;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
