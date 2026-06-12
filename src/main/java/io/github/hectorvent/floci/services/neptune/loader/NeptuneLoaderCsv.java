package io.github.hectorvent.floci.services.neptune.loader;

import io.github.hectorvent.floci.core.common.CsvParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates Neptune bulk-loader Gremlin CSV files into Gremlin scripts.
 *
 * <p>Vertex files carry {@code ~id} and {@code ~label} columns; edge files additionally
 * carry {@code ~from} and {@code ~to}. Property columns may declare a type suffix
 * ({@code name:String}, {@code age:Int}, ...) following the Neptune bulk load format.
 */
final class NeptuneLoaderCsv {

    record ParsedFile(boolean edges, List<String> rowScripts, int parsingErrors) {}

    private NeptuneLoaderCsv() {
    }

    static ParsedFile parse(String content) {
        List<List<String>> rows = CsvParser.parseAll(content);
        if (rows.isEmpty()) {
            return new ParsedFile(false, List.of(), 0);
        }
        List<String> header = rows.get(0);
        boolean edges = header.contains("~from");
        List<String> scripts = new ArrayList<>();
        int parsingErrors = 0;
        for (List<String> row : rows.subList(1, rows.size())) {
            if (row.size() == 1 && row.get(0).isBlank()) {
                continue;
            }
            String script = edges ? edgeScript(header, row) : vertexScript(header, row);
            if (script == null) {
                parsingErrors++;
                continue;
            }
            scripts.add(script);
        }
        return new ParsedFile(edges, scripts, parsingErrors);
    }

    private static String vertexScript(List<String> header, List<String> row) {
        String id = columnValue(header, row, "~id");
        String label = columnValue(header, row, "~label");
        if (id == null || id.isBlank()) {
            return null;
        }
        StringBuilder sb = new StringBuilder("g.addV(")
                .append(groovyString(label == null || label.isBlank() ? "vertex" : label))
                .append(").property(T.id, ").append(groovyString(id)).append(")");
        appendProperties(sb, header, row);
        return sb.append(".iterate()").toString();
    }

    private static String edgeScript(List<String> header, List<String> row) {
        String id = columnValue(header, row, "~id");
        String label = columnValue(header, row, "~label");
        String from = columnValue(header, row, "~from");
        String to = columnValue(header, row, "~to");
        if (from == null || from.isBlank() || to == null || to.isBlank()
                || label == null || label.isBlank()) {
            return null;
        }
        StringBuilder sb = new StringBuilder("g.V(").append(groovyString(from))
                .append(").addE(").append(groovyString(label))
                .append(").to(__.V(").append(groovyString(to)).append("))");
        if (id != null && !id.isBlank()) {
            sb.append(".property(T.id, ").append(groovyString(id)).append(")");
        }
        appendProperties(sb, header, row);
        return sb.append(".iterate()").toString();
    }

    private static void appendProperties(StringBuilder sb, List<String> header, List<String> row) {
        for (int i = 0; i < header.size() && i < row.size(); i++) {
            String column = header.get(i);
            if (column.startsWith("~")) {
                continue;
            }
            String value = row.get(i);
            if (value == null || value.isEmpty()) {
                continue;
            }
            int colon = column.indexOf(':');
            String name = colon > 0 ? column.substring(0, colon) : column;
            String type = colon > 0 ? column.substring(colon + 1).toLowerCase() : "string";
            sb.append(".property(").append(groovyString(name)).append(", ")
              .append(groovyValue(type, value)).append(")");
        }
    }

    private static String groovyValue(String type, String value) {
        return switch (type) {
            case "byte", "short", "int" -> value.strip();
            case "long" -> value.strip() + "L";
            case "float" -> value.strip() + "f";
            // Groovy decimal literals default to BigDecimal; Neptune stores Double
            case "double" -> value.strip() + "d";
            case "bool", "boolean" -> Boolean.parseBoolean(value.strip()) ? "true" : "false";
            default -> groovyString(value);
        };
    }

    private static String columnValue(List<String> header, List<String> row, String column) {
        int index = header.indexOf(column);
        return index >= 0 && index < row.size() ? row.get(index) : null;
    }

    private static String groovyString(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
