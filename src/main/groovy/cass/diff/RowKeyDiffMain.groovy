package cass.diff

import argmap.ArgMap
import cass.drv.Drv

/**
 * Compares / "Diffs" two different tables. The tables must have matching row key AND column keys (thus would produce the
 * same consistent hash values). If a key exists in both tables, it is not reported.
 *
 * This does not diff data columns
 *
 * - specify first / second aka left / right aka old / new driver connections
 * - init the driver connections
 * - specify the keyspace/tablename in each server we are comparing. PK columns must match in name, type, and order
 * - optionally: a hash token subrange
 */

ArgMap argMap = new ArgMap(args: args)

String startToken = argMap.startToken
String stopToken = argMap.stopToken

String firstClusterContactNodes = argMap.firstClusterContactNodes
String firstClusterPort = argMap.firstClusterPort
String firstKeyspace = argMap.firstKeyspace
String firstTable = argMap.firstTable

String secondClusterContactNodes = argMap.secondClusterContactNodes
String secondClusterPort = argMap.secondClusterPort
String secondKeyspace = argMap.secondKeyspace
String secondTable = argMap.secondTable

argMap.checkAllArgsUsed()

Drv first = new Drv(autoStart: true, clusterContactNodes: firstClusterContactNodes, clusterPort: firstClusterPort).initDataSources()
Drv second = new Drv(autoStart: true, clusterContactNodes: secondClusterContactNodes, clusterPort: secondClusterPort).initDataSources()

RowKeyDiff pkdiff = new RowKeyDiff(
        left: first,
        leftKeyspace: firstKeyspace,
        leftTable: firstTable,
        right: second,
        rightKeyspace: secondKeyspace,
        rightTable: secondTable,
        startToken: startToken,
        stopToken: stopToken,
)

pkdiff.diffReport(System.out)

