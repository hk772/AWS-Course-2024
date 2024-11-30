package org.example.Messages;

public class Message {
    public int localID;
    public String content;

    public Message(int id, String content) {
        this.localID = id;
        this.content = content;
    }

    public Message() {
    }

    public int getLocalID() {
        return localID;
    }

    public void setLocalID(int localID) {
        this.localID = localID;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
