package dags.sample.damoDecEnc;

import com.penta.scpdb.ScpDbAgent;

import java.lang.reflect.Method;

/**
 * D'amo(SafeDB) 동작만 빠르게 검증하는 CLI.
 *
 * - 인스턴스 생성: new ScpDbAgent()
 * - 호출: scpDecrypt(confPath, keyGroup, data) 또는 scpEncrypt(confPath, keyGroup, data)
 *
 * Reflection을 써서, 실제 라이브러리에 메서드가 있으면 호출하고
 * 없으면 "메서드 없음"으로 실패하게 합니다(컴파일 시점 의존성 최소화 목적).
 *
 * Usage:
 *   java -cp "<damo_jars_and_deps>;<compiled_classes_or_jar>" dags.sample.damoDecEnc.DamoScpCli <scpDecrypt|scpEncrypt> <confPath> <keyGroup> <data>
 *
 * Output:
 *   - stdout: 결과만 출력(앞/뒤 공백 없음)
 *   - stderr: 에러/스택트레이스
 */
public final class DamoScpCli {
    private DamoScpCli() {}

    public static void main(String[] args) {
        if (args == null || args.length < 4) {
            System.err.println("Usage: DamoScpCli <scpDecrypt|scpEncrypt> <confPath> <keyGroup> <data>");
            System.exit(2);
            return;
        }

        final String methodName = args[0];
        final String confPath = args[1];
        final String keyGroup = args[2];
        final String data = args[3];

        try {
            final ScpDbAgent ag = new ScpDbAgent();

            final Method m = ag.getClass().getMethod(methodName, String.class, String.class, String.class);
            final Object out = m.invoke(ag, confPath, keyGroup, data);

            if (out != null) {
                System.out.print(out.toString().trim());
            }
        } catch (NoSuchMethodException e) {
            System.err.println("Method not found: " + methodName + "(String,String,String)");
            e.printStackTrace(System.err);
            System.exit(3);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}

