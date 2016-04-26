package rank.page;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Wrapper {
    private static final String ACCESS_KEY_ID = "AKIAJ62G62YTENYHAHYA";
    private static final String SECRET_ACCESS_KEY = "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";
    private static final String BUCKET_NAME = "cs5300-hadoop-project";
    private static final AmazonS3 S3CLIENT = new AmazonS3Client(new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
    
    public static void listBuckets() {
        System.out.println("----- Buckets -----");
        for (Bucket b : S3CLIENT.listBuckets())
            System.out.println(" " + b);
        System.out.println();
    }
    
    public static void listElements(String bucketname) {
        if (S3CLIENT.doesBucketExist(bucketname)) {
            System.out.println("----- Bucket Contents -----");
            for (S3ObjectSummary s3ob : S3CLIENT.listObjects(bucketname).getObjectSummaries())
                System.out.println(" " + s3ob.getKey());
            System.out.println();
        }
    }
    
    public static void createBucket(String bucketname) {
        if (!S3CLIENT.doesBucketExist(bucketname)) {
            System.out.println("Creating bucket: " + bucketname);
            S3CLIENT.createBucket(bucketname);
            System.out.println("Bucket " + bucketname + " was successfully created.\n");
        }
    }
    
    public static void deleteBucket(String bucketname) {
        if (S3CLIENT.doesBucketExist(bucketname)) {
            System.out.println("Deleting bucket: " + bucketname);
            S3CLIENT.deleteBucket(bucketname);
            System.out.println("Bucket " + bucketname + " was deleted successfully.\n");
        }
    }
    
    public static void uploadFile(String bucketname, File file) {
        if (!S3CLIENT.doesBucketExist(bucketname))
            createBucket(bucketname);
        System.out.println("Uploading file: " + file.getName());
        S3CLIENT.putObject(bucketname, file.getName(), file);
        System.out.println("File " + file.getName() + " was uploaded successfully.\n");
    }
    
    public static File downloadFile(String bucketname, String filename) {
        if (S3CLIENT.doesBucketExist(bucketname) /*Check if file exists*/) {
            try {
                S3Object s3object = S3CLIENT.getObject(bucketname, filename);
                InputStream s3is = s3object.getObjectContent();
                
                File file = new File("res/" + filename + "-" + System.currentTimeMillis());
                FileOutputStream fos = new FileOutputStream(file);
                
                int bytesRead;
                byte[] buffer = new byte[1024];
                while ((bytesRead = s3is.read(buffer)) != -1)
                    fos.write(buffer, 0, bytesRead);
                
                fos.flush();
                fos.close();
                s3is.close();
                s3object.close();
                
                return file;
            }
            catch (IOException ex) {
                Logger.getLogger(S3Wrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }
}