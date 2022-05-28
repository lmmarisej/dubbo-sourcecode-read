package org.apache.dubbo.registry.zookeeper.dynamic;

import javassist.*;

import java.lang.reflect.Method;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 14:10
 */
public class JavassistMakeClassTest {
    public static void main(String[] args) throws Exception {
        ClassPool cp = ClassPool.getDefault(); // 创建 ClassPool
        // 要生成的类名称为 JavassistDemo
        CtClass clazz = cp.makeClass("org.apache.dubbo.registry.zookeeper.dynamic.JavassistDemo");

        StringBuilder body;
        // 创建字段，指定了字段类型、字段名称、字段所属的类
        CtField field = new CtField(cp.get("java.lang.String"), "prop", clazz);
        // 指定该字段使用 private 修饰
        field.setModifiers(Modifier.PRIVATE);

        // 设置 prop 字段的 getter/setter 方法
        clazz.addMethod(CtNewMethod.setter("setProp", field));
        clazz.addMethod(CtNewMethod.getter("getProp", field));
        // 设置 prop 字段的初始化值，并将 prop 字段添加到 clazz 中
        clazz.addField(field, CtField.Initializer.constant("MyName"));

        // 创建构造方法，指定了构造方法的参数类型和构造方法所属的类
        CtConstructor ctConstructor = new CtConstructor(new CtClass[]{}, clazz);
        // 设置方法体
        body = new StringBuilder();
        body.append("{\n prop=\"MyName\";\n}");
        ctConstructor.setBody(body.toString());
        clazz.addConstructor(ctConstructor); // 将构造方法添加到 clazz 中

        // 创建 execute() 方法，指定了方法返回值、方法名称、方法参数列表以及
        // 方法所属的类
        CtMethod ctMethod = new CtMethod(CtClass.voidType, "execute", new CtClass[]{}, clazz);
        // 指定该方法使用 public 修饰
        ctMethod.setModifiers(Modifier.PUBLIC);
        // 设置方法体
        body = new StringBuilder();
        body.append("{\n System.out.println(\"execute():\" + this.prop);");
        body.append("\n}");
        ctMethod.setBody(body.toString());
        clazz.addMethod(ctMethod); // 将 execute() 方法添加到 clazz 中
        // 将上面定义的 JavassistDemo 类保存到指定的目录
        clazz.writeFile("/Users/lmmarise.j/develop/java_web_project_list/dubbo-sourcecode-read/dubbo-registry/dubbo-registry-zookeeper/src/test/java/");
        // 加载 clazz 类，并创建对象
        Class<?> c = clazz.toClass();
        Object o = c.newInstance();
        // 调用 execute() 方法
        Method method = o.getClass().getMethod("execute");
        method.invoke(o);
    }
}
