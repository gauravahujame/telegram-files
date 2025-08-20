package telegram.files;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import telegram.files.repository.FileRecord;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class FileRepositoryRecoveryTest {

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        // ASSUMPTION: For tests we deploy DataVerticle to initialize DB and repositories
        vertx.deployVerticle(new DataVerticle())
                .onComplete(testContext.succeedingThenComplete());
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        DataVerticleTest.clear(vertx)
                .onComplete(v -> {
                    if (v.failed()) {
                        testContext.failNow(v.cause());
                    } else {
                        testContext.completeNow();
                    }
                });
    }

    @Test
    void testResetAllDownloadingToIdle(Vertx vertx, VertxTestContext testContext) {
        // DESIGN NOTE: Test recovery method for stuck downloads
        FileRecord downloadingFile = createTestFileRecord(1, "downloading_file.txt", FileRecord.DownloadStatus.downloading);
        FileRecord completedFile = createTestFileRecord(2, "completed_file.txt", FileRecord.DownloadStatus.completed);
        FileRecord idleFile = createTestFileRecord(3, "idle_file.txt", FileRecord.DownloadStatus.idle);

        DataVerticle.fileRepository.create(downloadingFile)
                .compose(v -> DataVerticle.fileRepository.create(completedFile))
                .compose(v -> DataVerticle.fileRepository.create(idleFile))
                .compose(v -> DataVerticle.fileRepository.resetAllDownloadingToIdle())
                .onComplete(testContext.succeeding(resetCount -> testContext.verify(() -> {
                    // Should reset 1 downloading file
                    assertEquals(1, resetCount, "Should reset 1 downloading file");
                    
                    // Verify the downloading file was reset to idle
                    DataVerticle.fileRepository.getByUniqueId(downloadingFile.uniqueId())
                            .onComplete(testContext.succeeding(updatedFile -> testContext.verify(() -> {
                                assertNotNull(updatedFile);
                                assertTrue(updatedFile.isDownloadStatus(FileRecord.DownloadStatus.idle));
                                assertEquals(0L, updatedFile.downloadedSize());
                                testContext.completeNow();
                            })));
                })));
    }

    @Test 
    void testResetStaleDownloadsToIdle(Vertx vertx, VertxTestContext testContext) {
        // DESIGN NOTE: Test watchdog method for stale downloads
        long currentTime = System.currentTimeMillis();
        long staleTime = currentTime - (20 * 60 * 1000); // 20 minutes ago (stale)
        long recentTime = currentTime - (5 * 60 * 1000); // 5 minutes ago (recent)

        FileRecord staleFile = createTestFileRecordWithStartDate(1, "stale_file.txt", 
            FileRecord.DownloadStatus.downloading, staleTime);
        FileRecord recentFile = createTestFileRecordWithStartDate(2, "recent_file.txt", 
            FileRecord.DownloadStatus.downloading, recentTime);

        DataVerticle.fileRepository.create(staleFile)
                .compose(v -> DataVerticle.fileRepository.create(recentFile))
                .compose(v -> DataVerticle.fileRepository.resetStaleDownloadsToIdle(15)) // 15 minute threshold
                .onComplete(testContext.succeeding(resetCount -> testContext.verify(() -> {
                    // Should reset 1 stale file
                    assertEquals(1, resetCount, "Should reset 1 stale downloading file");
                    testContext.completeNow();
                })));
    }

    @Test
    void testCountByStatusAfterReset(Vertx vertx, VertxTestContext testContext) {
        // DESIGN NOTE: Test that count methods work correctly after recovery
        FileRecord downloading1 = createTestFileRecord(1, "downloading1.txt", FileRecord.DownloadStatus.downloading);
        FileRecord downloading2 = createTestFileRecord(2, "downloading2.txt", FileRecord.DownloadStatus.downloading);
        FileRecord completed = createTestFileRecord(3, "completed.txt", FileRecord.DownloadStatus.completed);

        DataVerticle.fileRepository.create(downloading1)
                .compose(v -> DataVerticle.fileRepository.create(downloading2))
                .compose(v -> DataVerticle.fileRepository.create(completed))
                .compose(v -> DataVerticle.fileRepository.countByStatus(123L, FileRecord.DownloadStatus.downloading))
                .onComplete(testContext.succeeding(initialCount -> testContext.verify(() -> {
                    assertEquals(2, initialCount, "Should have 2 downloading files initially");
                    
                    // Reset stuck downloads
                    DataVerticle.fileRepository.resetAllDownloadingToIdle()
                            .compose(v -> DataVerticle.fileRepository.countByStatus(123L, FileRecord.DownloadStatus.downloading))
                            .onComplete(testContext.succeeding(afterResetCount -> testContext.verify(() -> {
                                assertEquals(0, afterResetCount, "Should have 0 downloading files after reset");
                                
                                // Verify idle count increased
                                DataVerticle.fileRepository.countByStatus(123L, FileRecord.DownloadStatus.idle)
                                        .onComplete(testContext.succeeding(idleCount -> testContext.verify(() -> {
                                            assertEquals(2, idleCount, "Should have 2 idle files after reset");
                                            testContext.completeNow();
                                        })));
                            })));
                })));
    }

    private FileRecord createTestFileRecord(int id, String fileName, FileRecord.DownloadStatus status) {
        // DESIGN NOTE: FileRecord signature is (id, uniqueId, telegramId, chatId, messageId, mediaAlbumId,
        //  date:int, hasSensitiveContent, size, downloadedSize, type, mimeType, fileName, thumbnail,
        //  thumbnailUniqueId, caption, extra, localPath, downloadStatus, transferStatus,
        //  startDate:long, completionDate:Long, tags, threadChatId:long, messageThreadId:long, reactionCount:long)
        int dateSec = (int) (System.currentTimeMillis() / 1000); // ASSUMPTION: seconds since epoch fits in int for tests
        long startDate = System.currentTimeMillis();
        return new FileRecord(
                id, "unique_" + id, 123L, 456L, id, 0L,
                dateSec, false, 1000L, 0L,
                "file", "text/plain", fileName, null, null,
                "Test caption", "Test extra", "/test/path/" + fileName,
                status.name(), FileRecord.TransferStatus.idle.name(),
                startDate, null, "test", 0L, 0L, 0L
        );
    }

    private FileRecord createTestFileRecordWithStartDate(int id, String fileName, 
            FileRecord.DownloadStatus status, long startDate) {
        int dateSec = (int) (System.currentTimeMillis() / 1000);
        return new FileRecord(
                id, "unique_" + id, 123L, 456L, id, 0L,
                dateSec, false, 1000L, 0L,
                "file", "text/plain", fileName, null, null,
                "Test caption", "Test extra", "/test/path/" + fileName,
                status.name(), FileRecord.TransferStatus.idle.name(),
                startDate, null, "test", 0L, 0L, 0L
        );
    }
}
