package org.embulk.output.wendelin;




import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.Consts;
import org.apache.http.client.utils.URIBuilder;

//import org.apache.http.auth.AuthScope;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.util.EntityUtils;
//import org.apache.http.entity.FileEntity;
//import org.apache.http.entity.ContentType;
//import org.apache.commons.codec.binary.Base64;

//import java.io.File;
//import java.nio.charset.CodingErrorAction;
//import org.apache.http.entity.StringEntity;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import java.net.UnknownHostException;

class WendelinClient
{
    private String basic_uri;
    private UsernamePasswordCredentials creds;
    private CloseableHttpClient httpclient;
    
    public WendelinClient(String streamtool_uri, String user, String password)
    {
        this.basic_uri = streamtool_uri + "/ingest" ;
        this.creds = new UsernamePasswordCredentials(user,password);
        this.httpclient = HttpClients.createDefault();
    }
    
    public void ingest(String reference, byte[] data_chunk) throws Exception
    {
        //CloseableHttpClient httpclient = HttpClients.createDefault();
        
        //String tag = "sample_01.txt";
        URI uri = new URIBuilder(basic_uri)
            .setParameter("reference",reference)
            .build();
            
        
        List<NameValuePair> formparams = new ArrayList<NameValuePair>();
        formparams.add(new BasicNameValuePair("data_chunk", new String(data_chunk)));
        
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formparams, Consts.UTF_8);
        
        
        HttpPost hppost = new HttpPost(uri);
        Header header = new BasicScheme(Consts.UTF_8).authenticate(creds , hppost, null);
        hppost.addHeader( header );

        hppost.setEntity(entity);
        
        try {
            
            System.out.println("Executing request "+ hppost.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(hppost);
            
            try{
                System.out.println("------------------------------------");
                System.out.println(response.getStatusLine());
                //System.out.println(EntityUtils.toString(response.getEntity()));
            } finally {
                response.close();
            }
        } catch(UnknownHostException ex) {
            System.out.println("catch the UnknowHostException");
            throw ex;
        }
    }
    
    public void close()
    {
        try {
            httpclient.close();
        } catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }
}