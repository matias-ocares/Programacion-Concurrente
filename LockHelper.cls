public class LockHelper {
    public static Boolean isLockError(Exception e) {
        return e.getMessage() != null && e.getMessage().contains('UNABLE_TO_LOCK_ROW');
    }
}