package io.ossim.omar.scdf.s3.uploader

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.GetObjectRequest
import org.springframework.beans.factory.annotation.Autowired

@SpringBootApplication
@EnableBinding(Processor.class)
@Slf4j
class OmarScdfS3UploaderApplication
{
    @Value('${s3Bucket:scdf-uploads}')
    String s3Bucket

    @Autowired
    AmazonS3Client s3Client

    /***********************************************************
    *
    * Function: main
    * Purpose:  main method of the class.
    *
    * @param    args (String[])
    *
    ***********************************************************/
    static final void main(String[] args)
    {
        SpringApplication.run OmarScdfS3UploaderApplication, args
    } // end method main

    @StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
    final String upLoadFolder(final Message<?> message)
    {
        log.debug("Received message ${message}")
        final String directory

        if(null != message.payload)
        {
            log.debug("Message payload: ${message.payload}")
            final def parsedJson = new JsonSlurper().parseText(message.payload)
            final File fileToUpload = new File(parsedJson.filename)

            if(!fileToUpload.isDirectory()){
                final String fileFullPath = fileToUpload.getAbsolutePath()
                final String s3Key = fileFullPath[1..fileFullPath.length()-1]
                s3Client.putObject(s3Bucket, s3Key, fileToUpload)
            }

            final JsonBuilder filename = new JsonBuilder()
            filename(filename : parsedJson.filename)
            log.debug("Message Sent: ${filename.toString()}")
            return filename.toString()
        }
    }
} // end class OmarScdfS3UploaderApplication
