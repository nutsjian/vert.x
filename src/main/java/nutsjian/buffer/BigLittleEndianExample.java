package nutsjian.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 关于字节序，字节序跟 CPU 有关
 *  JVM 会根据底层操作系统和CPU 自动转换字节序，所以我们使用 Java 进行网络编程，几乎感觉不到字节序的存在
 *
 *  Java 网络编程中默认使用的字节序是 大端字节序，如果 JVM 检测到操作系统和CPU是 小端字节序，则会自动转换
 */
public class BigLittleEndianExample {

  public static void main(String[] args) {
    // 公司电脑是 LITTLE_ENDIAN
    System.out.println(ByteOrder.nativeOrder());    // 通过该方法查看本机的 CPU 是小端 还是 大端

    // 我们也可以改变 JVM 中的默认字节序
    int x = 0x01020304;

    ByteBuffer bb = ByteBuffer.wrap(new byte[4]);
    bb.asIntBuffer().put(x);
    String ss_before = Arrays.toString(bb.array());
    // 可见 ByteBuffer 默认使用的是 大端字节序，也就是 Java 的 nio 网络中默认使用的是 大端字节序
    System.out.println("默认字节序 " +  bb.order().toString() +  ","  +  " 内存数据 " +  ss_before);

    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.asIntBuffer().put(x);
    String ss_after = Arrays.toString(bb.array());
    System.out.println("修改字节序 " + bb.order().toString() +  ","  +  " 内存数据 " +  ss_after);

  }

}
