package demo;

/**
 * Created by wangcr on 2017/1/24.
 */
public class Demo {
    public static void main(String[] args) {
        String jsonJobFile;
        if (args.length == 0) {
            jsonJobFile = "./src/main/resources/tables.json";
        } else {
            jsonJobFile = args[0];
            if (jsonJobFile.compareToIgnoreCase("-h") == 0) {
                System.out.println("Param: [json file] [time period to print account]");
                System.exit(0);
            }
        }

        TableReader tableReader = TableReader.getInstance(jsonJobFile);
        TableTransJob transJob = tableReader.getTableTransJob();
        if (transJob == null) {
            System.err.println("Json file may be not found or in wrong format!");
            System.exit(-1);
        }
        TransJobService transJobService = new TransJobService(transJob);
        if (args.length == 2) {
            TransJobService.reportInterval = Integer.parseInt(args[1]);
        }
        transJobService.jobDispatcher();
    }
}
