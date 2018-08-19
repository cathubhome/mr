package chapter1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created with IDEA
 * Author:catHome
 * Description: 自定义MR数据类型
 * Time:Create on 2018/8/13 14:23
 */
public class CustomWritable implements WritableComparable<CustomWritable>{

    private Integer id;

    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){ return true;}
        if (!(o instanceof CustomWritable)){ return false;}
        CustomWritable that = (CustomWritable) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return id +"\t"+ name;
    }

    @Override
    public int compareTo(CustomWritable o) {
        int comp = Integer.valueOf(this.getId()).compareTo(o.getId());
        if(0 != comp)
            return comp;
        return this.getName().compareTo(o.getName());
    }

    /**
     * 特别注意：read()与readFields()方法读写字段的顺序必须保持一致，不一致将会出错
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();
    }
}
