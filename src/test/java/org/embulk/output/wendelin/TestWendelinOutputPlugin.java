package org.embulk.output.wendelin;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Types;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.type.Type;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;

import org.mockito.ArgumentCaptor;

import org.embulk.output.wendelin.WendelinOutputPlugin.PluginTask;

//import org.embulk.config.TaskSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Arrays;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.*;


public class TestWendelinOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;
    private Schema schema;
    private WendelinOutputPlugin plugin;     // Wendelin plugin is Not Mocked but the WendelinClinet is Mocked
    private WendelinClient mClient;
    
    @Before
    public void createResources() throws Exception
    {
        
        schema = schema("payload",Types.STRING,"tag",Types.STRING);
        config = config();

        plugin = new WendelinOutputPlugin();
        mClient = mock(WendelinClient.class);
        plugin.setClient(mClient);
    }
    
    @Test
    public void testTransaction() throws Exception
    {
        
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run (TaskSource tasksource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        
        assertEquals("user","user");
        //assertEquals("password",plugin.password);
        //assertEquals("streamtool_uri",plugin.streamtool_uri);
        
        verify(mClient,times(1)).close();



        PluginTask task = pluginTask(config);
        
        // No errors happens, we check if we could have something happen in the Client
        TransactionalPageOutput output = plugin.open(task.dump(),schema,0);
        // We need to mock a mockPage.
        
        ArgumentCaptor<String> reference = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> data_chunk = ArgumentCaptor.forClass(byte[].class);
        
        
        String base64 = Base64.encodeBase64String("thedata".getBytes());
        List<String> lst = Arrays.asList(base64,"filename");
        Page page = Page.allocate(100).setStringReferences(lst);
        
        output.add(page);
        verify(mClient).ingest(reference.capture(),data_chunk.capture());
        //System.out.println("The reference:" + reference.getValue());
        //System.out.println("The data_chunk:" + new String(data_chunk.getValue()));
        
        
        assertEquals("weather-ccfilename",reference.getValue());
        assertEquals("thedata",new String(data_chunk.getValue()));
        
    }
    
    public static ConfigSource config()
    {
        return Exec.newConfigSource()
            .set("type","wendelin")
            .set("tag","weather-cc")
            .set("streamtool_uri","https://softinst80609.host.vifib.net/erp5/portal_ingestion_policies/weather-cc")
            .set("user","zope")
            .set("password","eidxkuns");
    }
    
    public static Schema schema(Object... nameAndType)
    {
        Schema.Builder builder = Schema.builder();
        for (int i=0; i < nameAndType.length; i += 2 ){
            String name = (String) nameAndType[i];
            Type type = (Type) nameAndType[i+1];
            builder.add(name,type);
        }
        return builder.build();
    }
    
    public static PluginTask pluginTask(ConfigSource config)
    {
        return config.loadConfig(PluginTask.class);
    }
    
}
