/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hazlecast.app.data;

import com.hazelcast.core.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.function.DistributedFunction;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import java.util.Map;

/**
 *
 * @author Denys.Prokopiuk
 */
public class MyApp {
    
    public static void main(String[] args) {
        
        JetInstance jet = Jet.newJetInstance();
        String name = jet.getName();
        
        Pipeline p = Pipeline.create();
        BatchSource batchSource = Sources.<String>list("text");
        
        BatchStage<String> batchStage = p.drawFrom(batchSource);
        
        BatchStage<String> batchStage2 = batchStage.flatMap((item)
                -> {
            String[] array = item.split("\\W+");
            Traverser<String> traverser = Traversers.traverseArray(array);
            return traverser;
        });
        
        BatchStage<String> batchStage3 = batchStage2.filter(item -> {
            try {
                System.out.println(name + "\tfilteringItem:" + item);                
                Thread.sleep(3000);
                return true;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        
         batchStage3.groupingKey(wholeItem());
        
        
        
        
        
        try {
            IList<String> text = jet.getList("text");
            text.add("A");
            text.add("B");

            //jet.newJob(null).join();
            Map<String, Long> counts = jet.getMap("result");
            
            for (String s : counts.keySet()) {
                System.out.println(name + ":\t " + s + "=" + counts.get(s));
            }
        } finally {
            Jet.shutdownAll();
        }
    }
    
}
