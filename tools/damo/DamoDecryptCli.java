package tools.damo;

import com.penta.scpdb.ScpDbAgent;

public final class DamoDecryptCli {
    private DamoDecryptCli() {}

    // Usage:
    //   java -cp "<damo_jars_and_deps>;<this_classes_dir_or_jar>" tools.damo.DamoDecryptCli <confPath> <keyGroup> <cipherText>
    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            System.err.println("Usage: DamoDecryptCli <confPath> <keyGroup> <cipherText>");
            System.exit(2);
            return;
        }

        final String confPath = args[0];
        final String keyGroup = args[1];
        final String cipherText = args[2];

        try {
            final ScpDbAgent ag = new ScpDbAgent();
            final String plain = ag.scpDecrypt(confPath, keyGroup, cipherText);
            if (plain != null) {
                System.out.print(plain);
            }
        } catch (Exception e) {
            // Keep stderr for debugging; stdout should remain clean for Python parsing.
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}

