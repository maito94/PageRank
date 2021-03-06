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
//    public static final String BUCKET_NAME = "edu-cornell-cs-cs5300s16-mag399-proj2";

    private static final AmazonS3 S3CLIENT = new AmazonS3Client(new BasicAWSCredentials(PageRank.ACCESS_KEY_ID, PageRank.SECRET_ACCESS_KEY));

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

    public static File downloadFile(String bucketname, String filename, boolean overwrite) {
        if (S3CLIENT.doesBucketExist(bucketname)) {
            try {
                S3Object s3object = S3CLIENT.getObject(bucketname, filename);
                InputStream s3is = s3object.getObjectContent();

                File file;
                if (overwrite)
                    file = new File("res/" + filename);
                else
                    file = new File("res/" + filename + "-" + System.currentTimeMillis());
                FileOutputStream fos = new FileOutputStream(file);

                System.out.println("Downloading file: " + filename + " --- Overwrite: " + overwrite);
                int bytesRead;
                byte[] buffer = new byte[1024];
                while ((bytesRead = s3is.read(buffer)) != -1)
                    fos.write(buffer, 0, bytesRead);

                fos.flush();
                fos.close();
                s3is.close();
                s3object.close();
                System.out.println("File " + filename + " was downloaded to " + file.getName() + " successfully.");

                return file;
            }
            catch (IOException ex) {
                Logger.getLogger(S3Wrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }
}