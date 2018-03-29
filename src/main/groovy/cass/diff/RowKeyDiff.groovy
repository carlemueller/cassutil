package cass.diff

import cass.diff.rs.RowKeyDiffResultSet
import cass.drv.Drv
import cass.drv.RsIterator
import cass.drv.St
import com.datastax.driver.core.ColumnMetadata
import groovy.transform.CompileStatic

@CompileStatic
class RowKeyDiff {
    
    Drv left
    String leftKeyspace
    String leftTable
    Drv right
    String rightKeyspace
    String rightTable

    String startToken 
    String stopToken 

    Iterator<DiffKey> iterator() {
        assert left?.initialized : "Left database access driver not configured or initialized"
        assert right?.initialized : "Right database access driver not configured or initialized"
        
        List<ColumnMetadata> rowkeyLeft = left?.cluster?.metadata?.getKeyspace(leftKeyspace)?.getTable(leftTable)?.partitionKey
        String rowKeyColsLeft = rowkeyLeft.collect { it.name }.join(',')

        List<ColumnMetadata> rowkeyRight = right?.cluster?.metadata?.getKeyspace(rightKeyspace)?.getTable(rightTable)?.partitionKey
        String rowKeyColsRight = rowkeyRight.collect { it.name }.join(',')

        assert rowKeyColsLeft && rowKeyColsLeft == rowKeyColsRight : "primary key column names do not match between tables or null key cols"

        String whereClause = "${startToken || stopToken ? 'WHERE':''} ${startToken ? 'token('+rowKeyColsLeft+') >= }'+startToken : ''} ${startToken && stopToken ? ' AND ' : ''} ${stopToken ? 'token('+rowKeyColsLeft+') <= }'+stopToken : ''}"
        String leftQuery = "SELECT DISTINCT token(${rowKeyColsLeft}),${rowKeyColsLeft} FROM ${leftKeyspace}.${leftTable} ${whereClause}"
        String rightQuery = "SELECT DISTINCT token(${rowKeyColsRight}),${rowKeyColsLeft} FROM ${rightKeyspace}.${rightTable} ${whereClause}"

        RowKeyDiffResultSet rs = new RowKeyDiffResultSet(
                leftPKMeta: rowkeyLeft,
                leftRS: left.execSync(new St(keyspace: leftKeyspace, cql: leftQuery)),
                rightPKMeta: rowkeyRight,
                rightRS: right.execSync(new St(keyspace: rightKeyspace, cql: rightQuery))
        )

        return new RsIterator(rs: rs)

    }

    void diffReport(Writer writer) {
        Iterator<DiffKey> iter = iterator()
        while (iter.hasNext()) {
            DiffKey dkey = iter.next()
            writer.write("${dkey.token}\t${dkey.key.join(',')}\t${dkey.side}\n")
        }
    }

}

