global with sharing class MembershipProcessorWithLocks implements Queueable {

    private List<Membership__c> batch;
    private Integer maxRetries;
    private Boolean skipOnFail;
    private Integer delayMs;
    private String scenario;

    public MembershipProcessorWithLocks(List<Membership__c> batch, Integer maxRetries, Boolean skipOnFail, Integer delayMs, String scenario) {
        this.batch = batch;
        this.maxRetries = maxRetries;
        this.skipOnFail = skipOnFail;
        this.delayMs = delayMs;
        this.scenario = scenario;
    }

    public void execute(QueueableContext qc) {
        Long startTime = System.currentTimeMillis();

        // Contadores para metricas
        Integer successCount = 0;
        Integer lockCount = 0;
        Integer skippedCount = 0;

        try{
            // 1. Recolectamos todos los códigos de cuenta
        Set<String> accountCodes = new Set<String>();
        for (Membership__c m : batch) {
            if (String.isNotBlank(m.Code__c)) {
                accountCodes.add(m.Code__c);
            }
        }

        // 2. Intentamos la query con FOR UPDATE y reintentos
        Map<String, Account> accountsMap = new Map<String, Account>();
        Integer attempts = 0;
        Boolean querySuccess = false;

        while (!querySuccess) {
            try {
                for (Account acc : [
                    SELECT Id, Name, Code__c, Counter__c
                    FROM Account
                    WHERE Code__c IN :accountCodes
                    FOR UPDATE
                ]) {
                    accountsMap.put(acc.Code__c, acc);
                }
                querySuccess = true; // si llegamos acá, la query funcionó

            } catch (QueryException qe) {
                attempts++;
                lockCount++;
                String errorMsg = qe.getMessage(); 
                //Logger.error(scenario + ' - ' + 'Error en query: ' + errorMsg);

                //Logger.warn(scenario+' - '+ 'Lock detectado al obtener Accounts. Intento ' + attempts);

                // Si llegamos al máximo y skipOnFail=true → cortar y saltar batch
                if (skipOnFail && attempts > maxRetries) {
                    Logger.warn(scenario+' - '+ 'Batch completo saltado tras ' + attempts + ' intentos fallidos');
                    skippedCount = batch.size();
                    Logger.saveLog();
                    return;
                }

                // Sii llegamos al maximo y skipOnFail=false --> reiniciamos el contador y seguimos intentando
                if (!skipOnFail && attempts > maxRetries) {
                    Logger.warn(scenario+' - '+ 'Llegamos al máximo pero seguimos intentando indefinidamente (skipOnFail=false)');
                    attempts = 0;
                }
            }
        }

        // 3. Si la query fue exitosa, procesamos Memberships
        List<Account> accountsToUpdate = new List<Account>();
        List<Membership__c> toInsert = new List<Membership__c>();

        if (querySuccess) {
            for (Membership__c m : batch) {
                Account acc = accountsMap.get(m.Code__c);
                if (acc != null) {
                    acc.Counter__c = (acc.Counter__c == null ? 0 : acc.Counter__c) + 1;

                    // simulo de trabajo pesado
                    /*for (Integer i = 0; i < 20000; i++) {
                        String dummy = acc.Name + i;
                    }*/
                    // Simulación de trabajo pesado
                    Long waitUntil = System.currentTimeMillis() + delayMs;
                    while (System.currentTimeMillis() < waitUntil) {
                        // busy wait simulando procesamiento largo dentro del lock
                    }

                    accountsToUpdate.add(acc);
                }

                m.Status__c = 'Procesado';
                toInsert.add(m);
                successCount++;
            }
        }

        // 4. Deduplicamos antes del update
        Map<Id, Account> dedupMap = new Map<Id, Account>();
        for (Account acc : accountsToUpdate) {
            dedupMap.put(acc.Id, acc);
        }

        // 5. DML bulkificado
        if (!dedupMap.isEmpty()) {
            update dedupMap.values();
        }
        if (!toInsert.isEmpty()) {
            insert toInsert;
        }

        // 6. Log final
        Long totalMs = System.currentTimeMillis() - startTime;
        Logger.info(scenario+' - '+ 'Batch finalizado. Éxitos=' + successCount +
                    ', Locks=' + lockCount +
                    ', Saltados=' + skippedCount +
                    ', Tiempo=' + totalMs + ' ms');
        Logger.saveLog();
            
        } catch (Exception e) {
            // 7. Log en caso de excepción inesperada
            Long totalMs = System.currentTimeMillis() - startTime;
            Logger.info(scenario+' - '+ 'Batch falló con excepción. Error=' + e.getMessage());
            Logger.saveLog();
            throw e; // opcional: relanzar si querés que el sistema lo vea como fallo
        }
    }
}
