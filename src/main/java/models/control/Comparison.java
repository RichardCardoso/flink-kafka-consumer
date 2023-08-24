package models.control;

import java.util.ArrayList;
import java.util.List;

public class Comparison {

    private List<ComparisonRule> comparisonRules;

    private ComparisonRange range;

    public List<ComparisonRule> getComparisonRules() {
        return comparisonRules;
    }

    public void setComparisonRules(List<ComparisonRule> comparisonRules) {
        this.comparisonRules = comparisonRules;
    }

    public ComparisonRange getRange() {
        return range;
    }

    public void setRange(ComparisonRange range) {
        this.range = range;
    }

    @Override
    public String toString() {
        return "Comparison{" +
                "comparisonRules=" + comparisonRules +
                ", range=" + range +
                '}';
    }
}
