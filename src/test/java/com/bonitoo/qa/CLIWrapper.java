package com.bonitoo.qa;

import com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.reader.StreamReader;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Scanner;
import java.util.Set;

public class CLIWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(CLIWrapper.class);


    public static String INFLUX2_SCRIPT = "./etc/influxdb2_community_test_env.sh";

    public enum SUDO {
        SUDO,
        NOT;
    }

    public enum WAIT {
        WAIT,
        CONTINUE;
    }

    private static SUDO DO_SUDO = SUDO.SUDO;
    private static WAIT DO_WAIT = WAIT.WAIT;
    private static int EXIT_VAL = 0;

    public static SUDO getDO_SUDO() {
        return DO_SUDO;
    }

    public static void setDO_SUDO(SUDO DO_SUDO) {
        DO_SUDO = DO_SUDO;
    }

    public static WAIT getDO_WAIT() {
        return DO_WAIT;
    }

    public static void setDO_WAIT(WAIT DO_WAIT) {
        DO_WAIT = DO_WAIT;
    }

    public static int getExitVal() {
        return EXIT_VAL;
    }

    public static void RUN_CMDX(String command, String ... args) throws NotImplementedException, IOException, InterruptedException {

        if(System.getProperty("os.name").startsWith("Windows")) {
            throw new UnsupportedOperationException("Windows not supported");
        }
        //tbd
        int cmdsLen = (DO_SUDO == SUDO.SUDO) ? args.length + 2 : args.length + 1;
        int argsIndex = 1;

        String [] cmds = new String[cmdsLen];

        if(DO_SUDO == SUDO.SUDO){
            cmds[0] = "sudo";
            cmds[1] = command;
            argsIndex = 2;
         }else{
            cmds[0] = command;
        }

        for(String arg : args){
            cmds[argsIndex] = arg;
            argsIndex++;
        }

        Process p = new ProcessBuilder()
                .redirectErrorStream(true)
                .command(cmds)
                .start();

        if(DO_WAIT == WAIT.WAIT){
            p.waitFor();
            EXIT_VAL = p.exitValue();
        }

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.getProperty("line.separator"));
        }


        String result = builder.toString();

        LOG.info(String.join(" ", cmds) + "\n" + result);

    }

    public static void WRITE_SCRIPT(String script, String scriptPath){

        try {
            if(!Files.exists(Paths.get(scriptPath))){
                Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxrw-rw-");
                FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions
                        .asFileAttribute(permissions);
                Files.createFile(Paths.get(scriptPath), fileAttributes);
            }

            File scriptFile = new File(scriptPath);
            FileOutputStream fos = new FileOutputStream(scriptFile);
            fos.write(script.getBytes());
            fos.close();
        }catch(FileNotFoundException e){
            LOG.warn(e.getMessage(), e);

        }catch(IOException e) {
            LOG.warn(e.getMessage(), e);
        }

    }

    public static String getInflux2Script() {
        return INFLUX2_SCRIPT;
    }

    public static String ExecReadToString(String execCommand) throws IOException {
        try (Scanner s = new Scanner(Runtime.getRuntime().exec(execCommand).getInputStream()).useDelimiter("\\A")) {
            return s.hasNext() ? s.next() : "";
        }
    }
}
