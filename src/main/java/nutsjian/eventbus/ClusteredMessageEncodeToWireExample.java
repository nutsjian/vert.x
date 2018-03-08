//package nutsjian.eventbus;
//
//import io.netty.util.CharsetUtil;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.net.impl.ServerID;
//
//import java.util.List;
//import java.util.Map;
//
//public class ClusteredMessageEncodeToWireExample {
//
//  public static void main(String[] args) {
//    int length = 1024; // TODO make this configurable
//    Buffer buffer = Buffer.buffer(length);
//    buffer.appendInt(0);
//    buffer.appendByte((byte) 1);
//    byte systemCodecID = messageCodec.systemCodecID();
//    buffer.appendByte(systemCodecID);
//    if (systemCodecID == -1) {
//      // User codec
//      writeString(buffer, messageCodec.name());
//    }
//    buffer.appendByte(send ? (byte)0 : (byte)1);
//    writeString(buffer, address);
//    String replyAddress = null;
//    if (replyAddress != null) {
//      writeString(buffer, replyAddress);
//    } else {
//      buffer.appendInt(0);
//    }
//    ServerID sender = new ServerID();
//    sender.host = "192.168.2.244";
//    sender.port = 8888;
//
//    buffer.appendInt(sender.port);
//    writeString(buffer, sender.host);
//    encodeHeaders(buffer);
//    writeBody(buffer);
//    buffer.setInt(0, buffer.length() - 4);
//  }
//
//  private static void writeString(Buffer buff, String str) {
//    byte[] strBytes = str.getBytes(CharsetUtil.UTF_8);
//    buff.appendInt(strBytes.length);
//    buff.appendBytes(strBytes);
//  }
//
//  private static void encodeHeaders(Buffer buffer) {
//    if (headers != null && !headers.isEmpty()) {
//      int headersLengthPos = buffer.length();
//      buffer.appendInt(0);
//      buffer.appendInt(headers.size());
//      List<Map.Entry<String, String>> entries = headers.entries();
//      for (Map.Entry<String, String> entry: entries) {
//        writeString(buffer, entry.getKey());
//        writeString(buffer, entry.getValue());
//      }
//      int headersEndPos = buffer.length();
//      buffer.setInt(headersLengthPos, headersEndPos - headersLengthPos);
//    } else {
//      buffer.appendInt(4);
//    }
//  }
//
//}
