import io.logz.sawmill.Main;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MainTest {

    @Test
    public void testHelloWorld() {
         assertThat(Main.helloWorld()).isEqualTo("Hello World");
    }
}
