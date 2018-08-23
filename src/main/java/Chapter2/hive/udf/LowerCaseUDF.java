package Chapter2.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created with IDEA
 * Author:catHome
 * Description: UDF用户自定义函数编程示例
 * Time:Create on 2018/8/23 11:50
 */
public class LowerCaseUDF extends UDF{

    /**
     *1、 UDF需继承org.apache.hadoop.hive.ql.exec.UDF
     *2、需实现evaluate函数，函数返回不能为void，支持重载
     *  字母转为小写
     * @param str
     * @return
     */
    public Text evaluate(Text str){
        if (str == null || str.toString() == null){
            return null;
        }
        return new Text(str.toString().toLowerCase());
    }
}
