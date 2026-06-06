import com.penta.scpdb.ScpDbAgent;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * 리플렉션 없이 디아모를 직접 호출하는 HTTP API.
 *
 * - GET /damo/enc/<평문>  -> 암호문(text/plain)
 * - GET /damo/dec/<암호문> -> 평문(text/plain)
 *
 * 환경변수:
 * - PORT (기본 8081)
 * - DAMO_CONF_PATH (기본 /data/app/airflow/damo/scp.ini)
 * - DAMO_GROUP (필수)
 */
public class DamoHttpApiDirect {
  private static final String CONF_PATH = getenv("DAMO_CONF_PATH", "/data/app/airflow/damo/scp.ini");
  private static final String GROUP = getenv("DAMO_GROUP", "KEY1");
  private static final ScpDbAgent AGENT = new ScpDbAgent();

  public static void main(String[] args) throws Exception {
    if (GROUP.isBlank()) {
      throw new IllegalStateException("Set DAMO_GROUP (KEY1)");
    }

    int port = port();
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(null);

    server.createContext("/damo/enc/", new EncHandler());
    server.createContext("/damo/dec/", new DecHandler());

    System.out.println("Listening " + port + "  GET /damo/enc/<plain>  GET /damo/dec/<cipher>");
    server.start();
  }

  static final class EncHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
      if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
        respond(ex, 405, "GET only\n");
        return;
      }
      String plain = tail(ex.getRequestURI(), "/damo/enc/");
      if (plain == null) {
        respond(ex, 400, "use /damo/enc/<plain>\n");
        return;
      }
      try {
        String cipher = AGENT.scpEncrypt(CONF_PATH, GROUP, plain);
        respond(ex, 200, cipher);
      } catch (Exception e) {
        respond(ex, 500, msg(e) + "\n");
      }
    }
  }

  static final class DecHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
      if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
        respond(ex, 405, "GET only\n");
        return;
      }
      String cipher = tail(ex.getRequestURI(), "/damo/dec/");
      if (cipher == null) {
        respond(ex, 400, "use /damo/dec/<cipher>\n");
        return;
      }
      try {
        String plain = AGENT.scpDecrypt(CONF_PATH, GROUP, cipher);
        respond(ex, 200, plain);
      } catch (Exception e) {
        respond(ex, 500, msg(e) + "\n");
      }
    }
  }

  private static String tail(URI uri, String prefix) {
    String path = uri.getPath();
    if (!path.startsWith(prefix)) return null;
    String t = path.substring(prefix.length());
    if (t.isEmpty()) return null;
    try {
      return URLDecoder.decode(t, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static void respond(HttpExchange ex, int code, String text) throws IOException {
    byte[] body = text.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
    ex.sendResponseHeaders(code, body.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(body);
    }
  }

  private static String getenv(String k, String d) {
    String v = System.getenv(k);
    if (v != null && !v.isBlank()) return v.trim();
    v = System.getProperty(k);
    if (v != null && !v.isBlank()) return v.trim();
    return d;
  }

  private static int port() {
    try {
      return Integer.parseInt(getenv("PORT", "8081"));
    } catch (NumberFormatException e) {
      return 8081;
    }
  }

  private static String msg(Exception e) {
    String m = e.getMessage();
    return (m == null || m.isBlank()) ? e.getClass().getSimpleName() : m;
  }
}

