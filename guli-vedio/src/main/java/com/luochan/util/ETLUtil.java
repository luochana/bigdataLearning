package com.luochan.util;

public class ETLUtil {
    public static String etlStr(String oriStr){

        StringBuffer sb = new StringBuffer();

        //1.切割
        String[] fields = oriStr.split("\t");

        //2.对字段长度进行过滤
        if(fields.length<9)
            return null;

        //3.去掉类别字段中的空格
        fields[3]=fields[3].replaceAll(" ","");

        //4.修改相关视频中的分割符, 由'\t'改为'&'
        for (int i = 0; i < fields.length; i++) {
            if(i<9){
                if(i==fields.length-1){
                    sb.append(fields[i]);
                }else {
                    sb.append(fields[i]).append("\t");
                }
            }else {
                //对相关视频ID进行处理
                if(i==fields.length-1){
                    sb.append(fields[i]);
                }else {
                    sb.append(fields[i]).append("&");
                }
            }
        }

        //5.返回结果

        return sb.toString();
    }

/*    public static void main(String[] args) {
        System.out.println(ETLUtil.etlStr("RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315\t474\tt60tW0WevkE\tWZgoejVDZlo\tXa_op4MhSkg\tMwynZ8qTwXA\tsfG2rtAkAcg\tj72VLPwzd_c\t24Qfs69Al3U"));
    }*/
}
