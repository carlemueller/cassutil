package cass.diff

import cass.diff.rs.FullRowDiffResultSet
import cass.drv.Drv
import cass.drv.RsIterator
import cass.drv.St
import com.datastax.driver.core.ColumnMetadata
import groovy.transform.CompileStatic

@CompileStatic
class FullRowDiff {
    
    Drv left
    String leftKeyspace
    String leftTable
    Drv right
    String rightKeyspace
    String rightTable

    List<String> dataColumns
    boolean dataWritetime = false // compare writetimes which may involve less data transfer

    String startToken 
    String stopToken 

    Iterator<DiffKey> iterator() {
        assert left?.initialized : "Left database access driver not configured or initialized"
        assert right?.initialized : "Right database access driver not configured or initialized"
        
        List<ColumnMetadata> rowkeyLeft = left?.cluster?.metadata?.getKeyspace(leftKeyspace)?.getTable(leftTable)?.partitionKey
        String rowKeyColsLeft = rowkeyLeft.collect { it.name }.join(',')

        List<ColumnMetadata> rowkeyRight = right?.cluster?.metadata?.getKeyspace(rightKeyspace)?.getTable(rightTable)?.partitionKey
        String rowKeyColsRight = rowkeyRight.collect { it.name }.join(',')

        List<ColumnMetadata> colkeyLeft = left?.cluster?.metadata?.getKeyspace(leftKeyspace)?.getTable(leftTable)?.clusteringColumns
        String colKeyColsLeft = colkeyLeft.collect { it.name }.join(',')

        List<ColumnMetadata> colkeyRight = right?.cluster?.metadata?.getKeyspace(rightKeyspace)?.getTable(rightTable)?.clusteringColumns
        String colKeyColsRight = colkeyRight.collect { it.name }.join(',')

        if (!dataColumns) {
            // default to all the data columns
            List<ColumnMetadata> cols = left?.cluster?.metadata?.getKeyspace(leftKeyspace)?.getTable(leftTable).columns
            cols = cols.findAll{ ColumnMetadata cmd -> rowkeyLeft.find{it.name == cmd.name} == null}.toList()
            cols = cols.findAll{ ColumnMetadata cmd -> colkeyLeft.find{it.name == cmd.name} == null}.toList()
            dataColumns = cols.collect{it.name}.toList()
        }
        if (dataWritetime) {
            for (int i=0; i <dataColumns.size(); i++) {
                if (!dataColumns[i].trim().toUpperCase().startsWith("WRITETIME")) {
                    dataColumns[i] = "WRITETIME(${dataColumns[i]})".toString()
                }
            }
        }

        assert rowKeyColsLeft && rowKeyColsLeft == rowKeyColsRight && colKeyColsLeft && colKeyColsLeft == colKeyColsRight :
                "primary key column names do not match between tables or null key cols"

        String whereClause = "${startToken || stopToken ? 'WHERE':''} ${startToken ? 'token('+rowKeyColsLeft+') >= }'+startToken : ''} ${startToken && stopToken ? ' AND ' : ''} ${stopToken ? 'token('+rowKeyColsLeft+') <= }'+stopToken : ''}"
        String leftQuery = "SELECT token(${rowKeyColsLeft}),${rowKeyColsLeft},${colKeyColsLeft},${dataColumns.join(',')} FROM ${leftKeyspace}.${leftTable} ${whereClause}"
        String rightQuery = "SELECT token(${rowKeyColsRight}),${rowKeyColsLeft},${colKeyColsRight},${dataColumns.join(',')} FROM ${rightKeyspace}.${rightTable} ${whereClause}"

        FullRowDiffResultSet rs = new FullRowDiffResultSet(
                leftRKMeta: rowkeyLeft,
                leftCKMeta: colkeyLeft,
                leftRS: left.execSync(new St(keyspace: leftKeyspace, cql: leftQuery)),
                rightRKMeta: rowkeyRight,
                rightCKMeta: colkeyRight,
                rightRS: right.execSync(new St(keyspace: rightKeyspace, cql: rightQuery)),
                dataColumns: dataColumns
        )

        return new RsIterator<DiffKey>(rs: rs)

    }

    void diffReport(Writer writer) {
        Iterator<DiffKey> iter = iterator()
        while (iter.hasNext()) {
            DiffKey dkey = iter.next()
            writer.write("${dkey.token}\t${dkey.key.join(',')}\t${dkey.side}\n")
        }
    }

}

