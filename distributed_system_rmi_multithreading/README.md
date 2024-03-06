### Intro

#### Start Server
Run with maven:
``` bash 
mvn clean install exec:exec
```

The starter of the rmi server is `org.example.FindImpl` with maven plugin:
``` xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <version>3.1.0</version>
    <configuration>
        <executable>java</executable>
        <arguments>
            <argument>-cp</argument>
            <classpath />
            <!--<argument>-Djava.security.policy=${project.build.outputDirectory}/rmi.policy</argument>-->
            <!-- point to the starter -->
            <argument>org.example.FindImpl</argument>
        </arguments>
    </configuration>
</plugin>
```


#### Invoke Remote Interface

Run test case `org.example.FindClient`.

