/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.usc;

/**
 *
 * @author Max He, Ph.D.
 */
public class Utils {

    // hom: 2, het: 1
    public static String getZygosity(String tmpGeno) {
        String geno;

        tmpGeno = tmpGeno.substring(0, tmpGeno.indexOf(":"));
        switch (tmpGeno) {
            case "0/0":
                geno = "0";
                break;
            case "0|0":
                geno = "0";
                break;
            case "0/1":
                geno = "1";
                break;
            case "0|1":
                geno = "1";
                break;
            case "1/0":
                geno = "1";
                break;
            case "1|0":
                geno = "1";
                break;
            case "0/2":
                geno = "1";
                break;
            case "0|2":
                geno = "1";
                break;
            case "2/0":
                geno = "1";
                break;
            case "2|0":
                geno = "1";
                break;
            case "1/1":
                geno = "2";
                break;
            case "1|1":
                geno = "2";
                break;
            case "1/2":
                geno = "1";
                break;
            case "1|2":
                geno = "1";
                break;
            case "2/1":
                System.err.println("\nWarning in getHomOrHetStatus with geno: " + tmpGeno + ".");
                geno = "1";
                break;
            case "2|1":
                geno = "1";
                break;
            case "2/2":
                System.err.println("\nWarning in getHomOrHetStatus with geno: " + tmpGeno + ".");
                geno = "2";
                break;
            case "2|2":
                geno = "2";
                break;
            default:
                geno = "-9";
                System.err.println("Error in getHomOrHetStatus with " + tmpGeno + ".  Exit...");
                System.exit(0);
                break;
        }

        return geno;
    }

    // snv: 1, indel: 2
    public static String getVarType(String ref, String alt) {
        String varType = "1";
        if ((!ref.equalsIgnoreCase("N")) && (ref.matches("^[AGCT\\.]+$") && alt.matches("^[AGCT\\.]+$"))) {
            if ((ref.length() == 1) && (alt.length() == 1)) {
                varType = "1";
            } else {
                varType = "2";
            }
        }

        return varType;
    }
}
