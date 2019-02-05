package com.bonitoo.qa;

import com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.reader.StreamReader;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

    public static void RUN_CMDX(SUDO useSudo, WAIT wait, String command, String ... args) throws NotImplementedException, IOException, InterruptedException {

        if(System.getProperty("os.name").startsWith("Windows")) {
            throw new UnsupportedOperationException("Windows not supported");
        }
        //tbd
        int cmdsLen = (useSudo == SUDO.SUDO) ? args.length + 2 : args.length + 1;
        int argsIndex = 1;

        String [] cmds = new String[cmdsLen];

        if(useSudo == SUDO.SUDO){
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

        if(wait == WAIT.WAIT){
            p.waitFor();
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

    }

    public static String getInflux2Script() {
        return INFLUX2_SCRIPT;
    }
}
