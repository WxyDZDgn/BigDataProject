package org.whania.experiment3.hbase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.whania.util.MyHBase;

import java.io.IOException;

public class TaskTest {
    @Before
    public void init() throws IOException {
        System.out.println("-------------init--------------");
        MyHBase.init();
        if(MyHBase.isExist("Course")) MyHBase.dropTable("Course");
        MyHBase.createTable("Course", "C_No", "C_Name", "C_Credit");
        MyHBase.insertRow("Course", "123001", "C_No", "data", "123001");
        MyHBase.insertRow("Course", "123001", "C_Name", "data", "Math");
        MyHBase.insertRow("Course", "123001", "C_Credit", "data", "2.0");
        MyHBase.insertRow("Course", "123002", "C_No", "data", "123002");
        MyHBase.insertRow("Course", "123002", "C_Name", "data", "Computer Science");
        MyHBase.insertRow("Course", "123002", "C_Credit", "data", "5.0");
        MyHBase.insertRow("Course", "123003", "C_No", "data", "123003");
        MyHBase.insertRow("Course", "123003", "C_Name", "data", "English");
        MyHBase.insertRow("Course", "123003", "C_Credit", "data", "3.0");
        System.out.println("-------------init--------------");
    }
    @Test
    public void task1() {}
    @Test
    public void task2() {}
    @Test
    public void task3() throws IOException {
        MyHBase.getData("Course", "123002", "C_Name", "");
    }
    @Test
    public void task4() throws IOException {
        MyHBase.insertRow("Course", "123004", "C_No", "data", "123004");
        MyHBase.insertRow("Course", "123004", "C_Name", "data", "Chemistry");
        MyHBase.insertRow("Course", "123004", "C_Credit", "data", "3.0");
    }
    @Test
    public void task5() throws IOException {
        MyHBase.deleteData("Course", "123001", "", "");
    }
    @After
    public void close() {
        MyHBase.close();
    }
}
