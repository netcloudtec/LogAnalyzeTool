package com.netcloud.loganaly.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WebLogBean implements Writable {

    /*
     * 来源IP
     */
    private String ip;
    /**
     * 访问时间
     */
    private String timeStr;
    /**
     * 请求方式
     */
    private String method;
    /**
     * 请求的url
     */
    private String request_url;
    /**
     * 使用的协议
     */
    private String protocol;
    /**
     * 状态码
     */
    private int status;
    /**
     * 字节数
     */
    private int bytes;
    /**
     * 来源url
     */
    private String from_url;
    /**
     * 使用的平台
     */
    private String platform;

    public WebLogBean() {

    }

    public WebLogBean(String ip, String timeStr, String method, String request_url, String protocol, int status, int bytes, String from_url, String platform) {
        this.ip = ip;
        this.timeStr = timeStr;
        this.method = method;
        this.request_url = request_url;
        this.protocol = protocol;
        this.status = status;
        this.bytes = bytes;
        this.from_url = from_url;
        this.platform = platform;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getRequest_url() {
        return request_url;
    }

    public void setRequest_url(String request_url) {
        this.request_url = request_url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }

    public String getFrom_url() {
        return from_url;
    }

    public void setFrom_url(String from_url) {
        this.from_url = from_url;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(this.timeStr);
        dataOutput.writeUTF(this.method);
        dataOutput.writeUTF(this.request_url);
        dataOutput.writeUTF(this.protocol);
        dataOutput.writeInt(this.status);
        dataOutput.writeInt(this.bytes);
        dataOutput.writeUTF(this.from_url);
        dataOutput.writeUTF(this.platform);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.ip = dataInput.readUTF();
        this.timeStr = dataInput.readUTF();
        this.method = dataInput.readUTF();
        this.request_url = dataInput.readUTF();
        this.protocol = dataInput.readUTF();
        this.status = dataInput.readInt();
        this.bytes = dataInput.readInt();
        this.from_url = dataInput.readUTF();
        this.platform = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "WebLogBean{" +
                "ip='" + ip + '\'' +
                ", timeStr='" + timeStr + '\'' +
                ", method='" + method + '\'' +
                ", request_url='" + request_url + '\'' +
                ", protocol='" + protocol + '\'' +
                ", status=" + status +
                ", bytes=" + bytes +
                ", from_url='" + from_url + '\'' +
                ", platform='" + platform + '\'' +
                '}';
    }
}
