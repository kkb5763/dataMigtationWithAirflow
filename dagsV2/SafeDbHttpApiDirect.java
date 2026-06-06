import com.initech.safedb.SimpleSafeDB;
import com.initech.safedb.sdk.exception.SafeDBSDKException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 리플렉션 없이 SafeDB를 직접 호출하는 HTTP API.
 *
 * - GET /safedb/enc/<평문>  -> 암호문(Base64, text/plain)
 * - GET /safedb/dec/<암호문> -> 평문(text/plain)
 *
 * 환경변수:
 * - PORT (기본 8084)
 * - SAFEDB_USER_NAME (기본 SAFEDB)
 * - SAFEDB_TABLE_NAME (기본 SAFEDB.POLICY)
 * - SAFEDB_COLUMN_NAME (기본 RSNO)
 *
 * classpath: SafeDB lib/*.jar + config/
 */
public class SafeDbHttpApiDirect {
  private static final String USER_NAME ="SAFEDB";
  private static final String TABLE_NAME = "SAFEDB.POLICY";
  private static final String COLUMN_NAME = "RSNO";
  private static final SimpleSafeDB SAFEDB = SimpleSafeDB.getInstance();
  private static volatile boolean loggedIn;

  public static void main(String[] args) throws Exception {
    int port = port();
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(null);

    server.createContext("/safedb/enc/", new EncHandler());
    server.createContext("/safedb/dec/", new DecHandler());

    System.out.println("Listening " + port + "  GET /safedb/enc/<plain>  GET /safedb/dec/<cipher>");
    server.start();
  }

  static final class EncHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
      if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
        respond(ex, 405, "GET only\n");
        return;
      }
      String plain = tail(ex.getRequestURI(), "/safedb/enc/");
      if (plain == null) {
        respond(ex, 400, "use /safedb/enc/<plain>\n");
        return;
      }
      try {
        String cipher = encrypt(plain);
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
      String cipher = tail(ex.getRequestURI(), "/safedb/dec/");
      if (cipher == null) {
        respond(ex, 400, "use /safedb/dec/<cipher>\n");
        return;
      }
      try {
        String plain = decrypt(cipher);
        respond(ex, 200, plain);
      } catch (Exception e) {
        respond(ex, 500, msg(e) + "\n");
      }
    }
  }

  private static String encrypt(String plain) throws SafeDBSDKException {
    ensureLogin();
    byte[] encrypted = SAFEDB.encrypt(USER_NAME, TABLE_NAME, COLUMN_NAME, plain.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(encrypted);
  }

  private static String decrypt(String cipherB64) throws SafeDBSDKException {
    ensureLogin();
    byte[] encrypted = Base64.getDecoder().decode(cipherB64);
    byte[] plain = SAFEDB.decrypt(USER_NAME, TABLE_NAME, COLUMN_NAME, encrypted);
    return new String(plain, StandardCharsets.UTF_8);
  }

  private static void ensureLogin() throws SafeDBSDKException {
    if (loggedIn) return;
    synchronized (SafeDbHttpApiDirect.class) {
      if (loggedIn) return;
      if (!SAFEDB.login()) {
        SAFEDB.getSafeDBConfigMgr().isLoginCheck();
      }
      loggedIn = true;
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
      return Integer.parseInt(getenv("PORT", "8084"));
    } catch (NumberFormatException e) {
      return 8084;
    }
  }

  private static String msg(Exception e) {
    String m = e.getMessage();
    return (m == null || m.isBlank()) ? e.getClass().getSimpleName() : m;
  }
}
