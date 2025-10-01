@RestResource(urlMapping='/InsertMembershipsWithLocks/*')
global with sharing class InsertMembershipsWithLocks {

    @HttpPost
    global static String post() {
        RestRequest req = RestContext.request;
        String body = req.requestBody != null ? req.requestBody.toString() : null;
        if (String.isBlank(body)) return 'Body vacío';

        // Parseamos parámetros opcionales del request
        Integer batchSize = getParamAsInt('batchSize', 200);     // Default 200
        Integer maxRetries = getParamAsInt('maxRetries', 3);      // Default 3
        Boolean skipOnFail = getParamAsBool('skipOnFail', true);  // Default true
        Integer delayMs = getParamAsInt('delayMs', 300);          // Default 300 ms artificial delay
        String scenario = getParamAsString('scenario', 'DefaultScenario');

        // Parseamos registros
        List<Membership__c> incoming;
        try {
            incoming = (List<Membership__c>) JSON.deserialize(body, List<Membership__c>.class);
        } catch (Exception e) {
            return 'JSON inválido: ' + e.getMessage();
        }
        if (incoming == null || incoming.isEmpty()) return 'No se recibieron registros';

        Logger.info('Inicio '+scenario+'. Total registros: ' + incoming.size() +
                    ', maxRetries=' + maxRetries +
                    ', skipOnFail=' + skipOnFail +
                    ', batchSize=' + batchSize +
                    ', delayMs=' + delayMs);

        // Fan-out
        for (Integer i = 0; i < incoming.size(); i += batchSize) {
            Integer endIdx = Math.min(i + batchSize, incoming.size());
            List<Membership__c> batch = new List<Membership__c>();
            for (Integer j = i; j < endIdx; j++) {
                batch.add(incoming[j]);
            }

            System.enqueueJob(new MembershipProcessorWithLocks(batch, maxRetries, skipOnFail, delayMs, scenario));
        }

        Logger.saveLog();
        return 'Fan-out lanzado para ' + incoming.size() + ' registros.';
    }

    // Helpers para leer parámetros del request
    private static Integer getParamAsInt(String name, Integer defaultVal) {
        String val = RestContext.request.params.get(name);
        return String.isNotBlank(val) ? Integer.valueOf(val) : defaultVal;
    }

    private static Boolean getParamAsBool(String name, Boolean defaultVal) {
        String val = RestContext.request.params.get(name);
        return String.isNotBlank(val) ? Boolean.valueOf(val) : defaultVal;
    }
    
    private static String getParamAsString(String name, String defaultVal) {
        String val = RestContext.request.params.get(name);
        return String.isNotBlank(val) ? val : defaultVal;
    }
}