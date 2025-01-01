package com.bsal;

import com.bsal.utils.ESUtil;

public class Main {

    public static void main(String[] args) {
        ESUtil esUtil = new ESUtil();
        // Creating an index on EDB
//        esUtil.createIndex("products");
        // Inserting
//        esUtil.insertIntoElasticDb();
        // Searching
        esUtil.searchElasticDb();
        // deleting
//        esUtil.deleteRecord();
//        esUtil.deleteIndex();
        esUtil.aggregationInElasticDb();
    }
}