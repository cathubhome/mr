package chapter1;

import utils.ReflectionUtil;

import java.lang.reflect.Method;

/**
 * Created with IDEA
 * Author:catHome
 * Description: 测试wordcount（亦可发布成jar包上传到服务器测试）
 * Time:Create on 2018/8/13 10:45
 */
public class WordCountTest {

    public static void main(String[] args) throws Exception {
        Class<?> clazz = Class.forName("chapter1.OptimizeWordCount");
        Method method = clazz.getMethod("main", String[].class);
        Object paramObj = (Object) new String[]{"/user/catonhometop/wordcount/input/word.txt", "/user/catonhometop/wordcount/output"};
        ReflectionUtil.invokeMethod(null,method,paramObj);
    }
}
