package io.github.hectorvent.floci.services.neptune.loader;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NeptuneLoaderCsvTest {

    @Test
    void vertexFileGeneratesAddVScripts() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~label,name:String,age:Int
                c1,customer,Alice,34
                c2,customer,Bob,41
                """);

        assertFalse(parsed.edges());
        assertEquals(0, parsed.parsingErrors());
        assertEquals(List.of(
                "g.addV('customer').property(T.id, 'c1').property('name', 'Alice').property('age', 34).iterate()",
                "g.addV('customer').property(T.id, 'c2').property('name', 'Bob').property('age', 41).iterate()"),
                parsed.rowScripts());
    }

    @Test
    void edgeFileGeneratesAddEScripts() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~from,~to,~label,total:Double
                e1,c1,o1,PLACED,99.5
                """);

        assertTrue(parsed.edges());
        assertEquals(List.of(
                "g.V('c1').addE('PLACED').to(__.V('o1')).property(T.id, 'e1').property('total', 99.5d).iterate()"),
                parsed.rowScripts());
    }

    @Test
    void untypedColumnsDefaultToString() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~label,city
                c1,customer,Berlin
                """);

        assertEquals(List.of(
                "g.addV('customer').property(T.id, 'c1').property('city', 'Berlin').iterate()"),
                parsed.rowScripts());
    }

    @Test
    void boolAndNumericTypesAreUnquoted() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~label,active:Bool,score:Long
                c1,customer,true,12345678901
                """);

        assertEquals(List.of(
                "g.addV('customer').property(T.id, 'c1').property('active', true).property('score', 12345678901L).iterate()"),
                parsed.rowScripts());
    }

    @Test
    void quotesAndBackslashesAreEscapedForGroovy() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~label,name:String
                c1,customer,"O'Brien \\ Co"
                """);

        assertEquals(List.of(
                "g.addV('customer').property(T.id, 'c1').property('name', 'O\\'Brien \\\\ Co').iterate()"),
                parsed.rowScripts());
    }

    @Test
    void emptyPropertyValuesAreSkipped() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,~label,name:String,age:Int
                c1,customer,,34
                """);

        assertEquals(List.of(
                "g.addV('customer').property(T.id, 'c1').property('age', 34).iterate()"),
                parsed.rowScripts());
    }

    @Test
    void rowsMissingRequiredColumnsCountAsParsingErrors() {
        NeptuneLoaderCsv.ParsedFile vertices = NeptuneLoaderCsv.parse("""
                ~id,~label,name:String
                ,customer,NoId
                c2,customer,Ok
                """);
        NeptuneLoaderCsv.ParsedFile edges = NeptuneLoaderCsv.parse("""
                ~id,~from,~to,~label
                e1,,o1,PLACED
                """);

        assertEquals(1, vertices.parsingErrors());
        assertEquals(1, vertices.rowScripts().size());
        assertEquals(1, edges.parsingErrors());
        assertEquals(0, edges.rowScripts().size());
    }

    @Test
    void vertexWithoutLabelDefaultsToVertex() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("""
                ~id,name:String
                c1,Alice
                """);

        assertEquals(List.of(
                "g.addV('vertex').property(T.id, 'c1').property('name', 'Alice').iterate()"),
                parsed.rowScripts());
    }

    @Test
    void emptyFileProducesNoScripts() {
        NeptuneLoaderCsv.ParsedFile parsed = NeptuneLoaderCsv.parse("");

        assertEquals(0, parsed.rowScripts().size());
        assertEquals(0, parsed.parsingErrors());
    }
}
