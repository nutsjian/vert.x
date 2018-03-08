package nutsjian.recordparser;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;

public class RecordParserExample01 {
  public static void main(String[] args) {
    RecordParser recordParser = RecordParser.newDelimited("\n", h -> {
      System.out.println(h.toString());
    });

    recordParser.handle(Buffer.buffer("HELLO\nHOW ARE Y"));
    recordParser.handle(Buffer.buffer("OU?\nI AM"));
    recordParser.handle(Buffer.buffer("DOING OK"));
    recordParser.handle(Buffer.buffer("\n"));
    recordParser.handle(Buffer.buffer("WHY I FEEL FUCK\nI CAME FROM"));
    recordParser.handle(Buffer.buffer("CHINA\n"));
  }
}
