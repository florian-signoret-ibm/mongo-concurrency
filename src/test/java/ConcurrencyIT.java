import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import mongo.MongoUtils;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

public class ConcurrencyIT {

    private static final String MONGO_DATABASE_NAME = "mongo-concurrency";
    private static final String MONGO_COLLECTION_NAME = "task";
    private static final String MONGO_TASK_NAME_COLUMN = "name";
    private static final String MONGO_TASK_STATUS_COLUMN = "status";
    private static final String TODO_STATUS = "TODO";
    private static final String IN_PROGRESS_STATUS = "IN_PROGRESS";
    private static final String DONE_STATUS = "DONE";

    private static MongoClient mongoClient;

    private static final int UPDATE_SLEEP_TIME_MILLISECONDS = 500;
    private static final int LIST_SLEEP_TIME_MILLISECONDS = 250;
    private static final int DOCUMENT_CREATION_ITERATION_COUNT = 100;
    private static final int UNDONE_DOCUMENT_COUNT = DOCUMENT_CREATION_ITERATION_COUNT * 2 + 1;

    @BeforeAll
    static void setup() {
        mongoClient = MongoUtils.createMongoClient();
        MongoDatabase db = mongoClient.getDatabase(MONGO_DATABASE_NAME);
        db.drop();
        db.createCollection(MONGO_COLLECTION_NAME);
        MongoCollection<Document> taskCollection = db.getCollection(MONGO_COLLECTION_NAME);
        taskCollection.createIndex(Indexes.ascending(MONGO_TASK_NAME_COLUMN), new IndexOptions().unique(true));
        taskCollection.createIndex(Indexes.ascending(MONGO_TASK_STATUS_COLUMN));

        // Create n tasks of each status

        for (int i = 0; i < DOCUMENT_CREATION_ITERATION_COUNT; ++i) {
            Document document = new Document();
            document.append(MONGO_TASK_NAME_COLUMN, "task-1-" + i);
            document.append(MONGO_TASK_STATUS_COLUMN, TODO_STATUS);
            taskCollection.insertOne(document);

            document = new Document();
            document.append(MONGO_TASK_NAME_COLUMN, "task-2-" + i);
            document.append(MONGO_TASK_STATUS_COLUMN, IN_PROGRESS_STATUS);
            taskCollection.insertOne(document);

            document = new Document();
            document.append(MONGO_TASK_NAME_COLUMN, "task-3-" + i);
            document.append(MONGO_TASK_STATUS_COLUMN, DONE_STATUS);
            taskCollection.insertOne(document);
        }

        Document myTask = new Document();
        myTask.append(MONGO_TASK_NAME_COLUMN, "my-task");
        myTask.append(MONGO_TASK_STATUS_COLUMN, TODO_STATUS);
        taskCollection.insertOne(myTask);
    }

    @Test
    public void testConcurrency() throws InterruptedException {

        ExecutorService service = Executors.newFixedThreadPool(2);
        TaskLister taskLister = new TaskLister();
        TaskUpdater taskUpdater = new TaskUpdater();

        service.submit(taskLister);
        service.submit(taskUpdater);

        while(!taskUpdater.isFinished()) {
            Thread.sleep(1000);
            assertNull(taskLister.getException());
            assertTrue(taskLister.isAllCountOk(), taskLister.getErrorMessage());
        }

        assertNull(taskLister.getException());
        assertTrue(taskLister.isAllCountOk(), taskLister.getErrorMessage());
        assertNull(taskUpdater.getException());
        assertTrue(taskUpdater.isSuccess());

        service.shutdownNow();
    }

    public static class TaskUpdater implements Callable<Void> {
        private Exception exception;
        private boolean isFinished = false;
        private boolean isSuccess = false;

        @Override
        public Void call() {
            try {
                for (int i = 0; i < 1000; ++i) {
                    updateMyTask(IN_PROGRESS_STATUS);
                    Thread.sleep(UPDATE_SLEEP_TIME_MILLISECONDS);
                    updateMyTask(TODO_STATUS);
                    Thread.sleep(UPDATE_SLEEP_TIME_MILLISECONDS);
                }
                isSuccess = true;
            } catch (Exception e) {
                exception = e;
            } finally {
                isFinished = true;
            }

            return null;
        }

        private void updateMyTask(String status) {
            MongoDatabase db = mongoClient.getDatabase(MONGO_DATABASE_NAME);
            MongoCollection<Document> taskCollection = db.getCollection(MONGO_COLLECTION_NAME);

            Document myTask = taskCollection.find(eq(MONGO_TASK_NAME_COLUMN, "my-task")).first();
            assertNotNull(myTask);
            Object id = myTask.get("_id");

            myTask.append(MONGO_TASK_STATUS_COLUMN, status);

            UpdateResult result = taskCollection.replaceOne(
                eq("_id", id), myTask, new ReplaceOptions().upsert(true)
            );
            assertEquals(1, result.getModifiedCount());
        }

        public Exception getException() {
            return exception;
        }

        public boolean isFinished() {
            return isFinished;
        }

        public boolean isSuccess() {
            return isSuccess;
        }
    }

    public static class TaskLister implements Callable<Void> {
        private Exception exception;
        private boolean allCountOk = true;
        private String errorMessage;

        @Override
        public Void call() {
            try {
                int queryCount = 0;
                while (allCountOk) {
                    ++queryCount;
                    Thread.sleep(LIST_SLEEP_TIME_MILLISECONDS);
                    int undoneCount = countUndoneTasks();
                    if (undoneCount != UNDONE_DOCUMENT_COUNT) {
                        allCountOk = false;
                        errorMessage = "Found " + undoneCount + " undone documents, but expected "
                                + UNDONE_DOCUMENT_COUNT + " (after " + queryCount + " queries)";
                    }
                }
            } catch (Exception e) {
                exception = e;
            }

            return null;
        }

        private static int countUndoneTasks() {
            MongoDatabase db = mongoClient.getDatabase(MONGO_DATABASE_NAME);
            MongoCollection<Document> taskCollection = db.getCollection(MONGO_COLLECTION_NAME);
            List<Document> documents = taskCollection.find(Filters.not(eq(MONGO_TASK_STATUS_COLUMN, DONE_STATUS)))
                    .into(new ArrayList<>());
            return documents.size();
        }

        public Exception getException() {
            return exception;
        }

        public boolean isAllCountOk() {
            return allCountOk;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}
