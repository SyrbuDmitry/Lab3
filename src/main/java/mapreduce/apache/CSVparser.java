package mapreduce.apache;

public class CSVparser {
    public static String[] splitter(String s,String regex){
        return s.split(regex);
    }
}
