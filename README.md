# cassutil

A grabbag of cassandra java-driver utilities and wrappers

## Driver wrapper

A class with various convenience methods that wraps the DSE java-driver, and works on the 
convenience structures that wrap statements in conjunction with timestamp, consistency, and other options

## Collating Result Sets

Result Sets that pull from two other result sets simultaneously, preserving order and resolving conflicts/merges

## Time Bucket mapping Result Sets

Result Sets that map to a set of tables representing time bucketed/sharded data.

## Time BUcket spanning Result Sets

Result Sets that span across time bucketed/sharded tables of data

## Autopaging Result Sets

Result Sets that wrap paging and logic to pull more results automatically. 

## Diffing sets

Utilities that detect differences in data between two tables (for after migrations or moving data around)

## Artifacts

Using jitpack:

    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>

    <dependency>
        <groupId>com.github.carlemueller</groupId>
        <artifactId>cassutil</artifactId>
        <version>1.0</version>
    </dependency>

gradle:

	repositories {
			...
	    maven { url 'https://jitpack.io' }
    }

    compile group: 'com.github.carlemueller', name: 'cassutil', version: '1.0'
