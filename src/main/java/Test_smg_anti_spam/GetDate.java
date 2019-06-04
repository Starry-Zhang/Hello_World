package Test_smg_anti_spam;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

public class GetDate {
    public static void main(String[] args) throws ParseException {

        System.out.println(getHour()==14);
    }

    /**
     * 返回小时
     * @param date
     * @return
     */
    public static int getHour() {
        Date date=new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 返回分钟
     * @param date
     * @return
     */
    public static int getMinute() {
        Date date=new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    }
    /**
     * 返回秒钟
     */
    public static int getSecond() {
        Date date=new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    }

}
