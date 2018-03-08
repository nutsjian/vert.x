package nutsjian.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class BufferExample01 {

  public static void main(String[] args) {
    // 使用Buffer的静态方法来创建一个 buffer，初始化内容是 hello world
    Buffer buffer1 = Buffer.buffer("hello world");
    System.out.println(buffer1.getString(0, 3));
    System.out.println(buffer1.getString(0, 4));

    buffer1.appendString("ilike");
    System.out.println(buffer1.getString(11, 16));

    // 会覆盖原来位置 1 开始起往后的 buffer
    buffer1.setString(1, "man");
    System.out.println(buffer1.getString(0, 10));

    // 因为 buffer1 中存的不是 json 字符串，所以导致 decode 失败，抛出异常
    JsonObject json = buffer1.toJsonObject();
    String value = json.getString("key");
    System.out.println(value);


//    Unpooled.unreleasableBuffer()
  }

}
