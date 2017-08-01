package org.embulk.output.wendelin;

//import WendelinClient;
//import org.embulk.EmbulkTestRuntime;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Page;
import org.embulk.spi.TransactionalPageOutput;

import org.slf4j.Logger;
import org.apache.commons.codec.binary.Base64;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.io.IOException;

public class WendelinOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {

        @Config("tag")
        @ConfigDefault("")
        public String getTag();


        @Config("streamtool_uri")
        public String getStreamtoolUri();

        @Config("user")
        @ConfigDefault("")
        public String getUser();
        
        @Config("password")
        @ConfigDefault("")
        public String getPassword();
        
    }
    
    private final Logger log = Exec.getLogger(getClass());
    private static WendelinClient wendelin=null;
    private static String tag;
    
    
    // This method is for unitTest, we set the WendelinClient to a mockClient
    public void setClient(WendelinClient wendelin)
    {
        this.wendelin = wendelin;
    }
    

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        
        tag = task.getTag();
        String streamtool_uri = task.getStreamtoolUri();
        String user = task.getUser();
        String passwd = task.getPassword();
        
        if (wendelin == null){
            wendelin = new WendelinClient(streamtool_uri,user,passwd);
        }
        control.run(task.dump());
        
        log.info("Closed the stream!");
        wendelin.close();
        

        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("wendelin output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new TransactionalPageOutput(){
          public void add(Page page){
            log.info("The ADD: " + page.getStringReferences() + " ## " +page.getValueReferences());
            try {
              String page_tag = tag + page.getStringReference(1).replaceAll(File.separator,".");
              wendelin.ingest(page_tag,Base64.decodeBase64(page.getStringReference(0)));
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          }
          
          public void finish(){
            log.info("Finished");
          }
          
          public void close(){
            log.info("closed");
          }
          
          public void abort(){
            
          }
          
          public TaskReport commit(){
            return Exec.newTaskReport();
            
          }
          
        };
    }
}