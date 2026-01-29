"""
Tests for analysis_helpers module.

Includes both unit tests with synthetic data and integration tests with real test database.
"""

import pytest
import pandas as pd
import os
import tempfile
from pymaude import MaudeDatabase
from pymaude import analysis_helpers


class TestAnalysisHelpers:
    """Unit tests with synthetic data."""

    def test_trends_for(self):
        """Test yearly trends calculation."""
        df = pd.DataFrame({
            'DATE_RECEIVED': ['2020-01-15', '2020-06-20', '2021-03-10', '2021-08-05', '2021-12-25'],
            'EVENT_TYPE': ['Death', 'Injury', 'Malfunction', 'Death', 'Injury']
        })

        trends = analysis_helpers.trends_for(df)

        assert len(trends) == 2  # 2020 and 2021
        assert trends['year'].tolist() == [2020, 2021]
        assert trends.loc[trends['year'] == 2020, 'event_count'].values[0] == 2
        assert trends.loc[trends['year'] == 2021, 'event_count'].values[0] == 3
        assert trends.loc[trends['year'] == 2020, 'deaths'].values[0] == 1
        assert trends.loc[trends['year'] == 2021, 'deaths'].values[0] == 1

    def test_trends_for_missing_columns(self):
        """Test trends_for with missing columns raises error."""
        df = pd.DataFrame({'DATE_RECEIVED': ['2020-01-15']})

        with pytest.raises(ValueError, match="missing required columns"):
            analysis_helpers.trends_for(df)

    def test_event_type_breakdown_for(self):
        """Test event type breakdown."""
        df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3, 4, 5],
            'EVENT_TYPE': ['Death', 'Injury', 'Malfunction', 'Death', 'Injury, Malfunction']
        })

        breakdown = analysis_helpers.event_type_breakdown_for(df)

        assert breakdown['total'] == 5
        assert breakdown['deaths'] == 2
        assert breakdown['injuries'] == 2
        assert breakdown['malfunctions'] >= 2  # At least 2

    def test_event_type_breakdown_deduplicates(self):
        """Test that event_type_breakdown deduplicates by MDR_REPORT_KEY."""
        df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 1, 2, 2, 3],  # Duplicates
            'EVENT_TYPE': ['Death', 'Death', 'Injury', 'Injury', 'Malfunction']
        })

        breakdown = analysis_helpers.event_type_breakdown_for(df)

        assert breakdown['total'] == 3  # Only 3 unique reports
        assert breakdown['deaths'] == 1
        assert breakdown['injuries'] == 1
        assert breakdown['malfunctions'] == 1

    def test_top_manufacturers_for(self):
        """Test top manufacturers calculation."""
        df = pd.DataFrame({
            'MANUFACTURER_D_NAME': ['Acme Corp', 'Acme Corp', 'Beta Inc', 'Gamma LLC', 'Beta Inc']
        })

        top_mfg = analysis_helpers.top_manufacturers_for(df, n=2)

        assert len(top_mfg) == 2
        assert top_mfg.iloc[0]['manufacturer'] in ['Acme Corp', 'Beta Inc']
        assert top_mfg.iloc[0]['event_count'] == 2

    def test_date_range_summary_for(self):
        """Test date range summary."""
        df = pd.DataFrame({
            'DATE_RECEIVED': ['2020-01-01', '2020-06-15', '2020-12-31']
        })

        summary = analysis_helpers.date_range_summary_for(df)

        assert summary['first_date'] == '2020-01-01'
        assert summary['last_date'] == '2020-12-31'
        assert summary['total_days'] == 365  # 2020 was a leap year
        assert summary['total_records'] == 3

    def test_standardize_brand_names(self):
        """Test brand name standardization."""
        df = pd.DataFrame({
            'BRAND_NAME': ['VENOVO', 'Venovo Stent', 'vici device', 'Unknown Product', None]
        })
        mapping = {'venovo': 'Venovo', 'vici': 'Vici'}

        result = analysis_helpers.standardize_brand_names(df, mapping)

        assert 'standard_brand' in result.columns
        # Check non-null values
        assert result['standard_brand'].iloc[0] == 'Venovo'
        assert result['standard_brand'].iloc[1] == 'Venovo'
        assert result['standard_brand'].iloc[2] == 'Vici'
        assert result['standard_brand'].iloc[3] == 'Unknown Product'
        # Check null value (pandas converts None to NaN)
        assert pd.isna(result['standard_brand'].iloc[4])

    def test_standardize_brand_names_custom_columns(self):
        """Test brand name standardization with custom column names."""
        df = pd.DataFrame({
            'custom_brand': ['VENOVO Product', 'VICI Device']
        })
        mapping = {'venovo': 'Venovo', 'vici': 'Vici'}

        result = analysis_helpers.standardize_brand_names(
            df, mapping, source_col='custom_brand', target_col='cleaned_brand'
        )

        assert 'cleaned_brand' in result.columns
        assert result['cleaned_brand'].tolist() == ['Venovo', 'Vici']

    def test_combine_device_names_search_groups_preserve_groups(self):
        data = {
            'search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C',
                'group-D',
                'group-D',
                'group-D',
                
            ],
            'id': [1, 2, 3, 4, 5, 6, 7, 8]
        }

        df = pd.DataFrame(data)
        
        result = analysis_helpers._combine_device_names_search_groups(df, ['group-A', 'group-B', 'group-C'])

        expected = {
            'search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C'
            ],
            'id': [1, 2, 3, 4, 5]
        }
        expected = pd.DataFrame(expected)

        pd.testing.assert_frame_equal(result, expected)


    def test_combine_device_names_search_groups_do_not_preserve_groups(self):
        data = {
            'search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C',
                'group-D',
                'group-D',
                'group-D',
                
            ],
            'id': [1, 2, 3, 4, 5, 6, 7, 8]
        }

        df = pd.DataFrame(data)
        
        result = analysis_helpers._combine_device_names_search_groups(df, ['group-A', 'group-B', 'group-C'], preserve_groups=False)

        expected = {
            'id': [1, 2, 3, 4, 5]
        }
        expected = pd.DataFrame(expected)

        pd.testing.assert_frame_equal(result, expected)


    def test_combine_device_names_search_groups_alt_group_col(self):
        data = {
            'other_search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C',
                'group-D',
                'group-D',
                'group-D',
                
            ],
            'id': [1, 2, 3, 4, 5, 6, 7, 8]
        }

        df = pd.DataFrame(data)
        
        result = analysis_helpers._combine_device_names_search_groups(df, ['group-A', 'group-B', 'group-C'], group_var='other_search_group')

        expected = {
            'other_search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C'
            ],
            'id': [1, 2, 3, 4, 5]
        }
        expected = pd.DataFrame(expected)

        pd.testing.assert_frame_equal(result, expected)


    def test_remap_device_groups_basic(self):
        """Test basic remapping with all groups mapped."""
        data = {
            'search_group': [
                'group-A',
                'group-B',
                'group-B',
                'group-B',
                'group-C',
                'group-D',
                'group-D',
                'group-D',
            ],
            'id': [1, 2, 3, 4, 5, 6, 7, 8]
        }

        df = pd.DataFrame(data)

        result = analysis_helpers.remap_device_groups(df, new_group_mapping={
            'group-I': 'group-A',
            'group-II': ['group-B', 'group-C', 'group-D']
        })

        expected = {
            'search_group': [
                'group-I',
                'group-II',
                'group-II',
                'group-II',
                'group-II',
                'group-II',
                'group-II',
                'group-II',
            ],
            'id': [1, 2, 3, 4, 5, 6, 7, 8]
        }
        expected = pd.DataFrame(expected)

        pd.testing.assert_frame_equal(result, expected)

    def test_remap_device_groups_single_str_value(self):
        """Test remapping with single string value (rename)."""
        df = pd.DataFrame({
            'search_group': ['old-name', 'old-name', 'other'],
            'id': [1, 2, 3]
        })

        result = analysis_helpers.remap_device_groups(df, new_group_mapping={
            'new-name': 'old-name',
            'other-new': 'other'
        })

        assert result['search_group'].tolist() == ['new-name', 'new-name', 'other-new']

    def test_remap_device_groups_missing_column_raises(self):
        """Test that missing group column raises ValueError."""
        df = pd.DataFrame({'other_col': [1, 2, 3]})

        with pytest.raises(ValueError, match="must contain 'search_group' column"):
            analysis_helpers.remap_device_groups(df, new_group_mapping={'new': 'old'})

    def test_remap_device_groups_duplicate_old_group_raises(self):
        """Test that mapping same old group to multiple new groups raises ValueError."""
        df = pd.DataFrame({
            'search_group': ['group-A', 'group-B'],
            'id': [1, 2]
        })

        with pytest.raises(ValueError, match="assigned to more than one new group"):
            analysis_helpers.remap_device_groups(df, new_group_mapping={
                'new-1': 'group-A',
                'new-2': ['group-A', 'group-B']  # group-A appears twice
            })

    def test_remap_device_groups_unmapped_raises_by_default(self):
        """Test that unmapped groups raise ValueError by default (allow_unspecified=False)."""
        df = pd.DataFrame({
            'search_group': ['group-A', 'group-B', 'group-C'],
            'id': [1, 2, 3]
        })

        with pytest.raises(ValueError, match="following groups are not remapped"):
            analysis_helpers.remap_device_groups(df, new_group_mapping={
                'new-group': 'group-A'
                # group-B and group-C are not mapped
            })

    def test_remap_device_groups_allow_unspecified_true(self):
        """Test that allow_unspecified=True passes through unmapped groups unchanged."""
        df = pd.DataFrame({
            'search_group': ['group-A', 'group-B', 'group-C', 'group-C'],
            'id': [1, 2, 3, 4]
        })

        result = analysis_helpers.remap_device_groups(
            df,
            new_group_mapping={'new-A': 'group-A'},
            allow_unspecified=True
        )

        # All rows preserved, unmapped groups keep original names
        assert len(result) == 4
        assert result['search_group'].tolist() == ['new-A', 'group-B', 'group-C', 'group-C']
        assert result['id'].tolist() == [1, 2, 3, 4]

    def test_remap_device_groups_new_group_column(self):
        """Test that new_group_column creates a new column instead of overwriting."""
        df = pd.DataFrame({
            'search_group': ['group-A', 'group-B'],
            'id': [1, 2]
        })

        result = analysis_helpers.remap_device_groups(
            df,
            new_group_mapping={
                'new-A': 'group-A',
                'new-B': 'group-B'
            },
            new_group_column='remapped_group'
        )

        # Original column preserved
        assert result['search_group'].tolist() == ['group-A', 'group-B']
        # New column has remapped values
        assert result['remapped_group'].tolist() == ['new-A', 'new-B']

    def test_remap_device_groups_custom_group_var(self):
        """Test remapping with custom group_var column name."""
        df = pd.DataFrame({
            'custom_group': ['A', 'B', 'B'],
            'id': [1, 2, 3]
        })

        result = analysis_helpers.remap_device_groups(
            df,
            new_group_mapping={'combined': ['A', 'B']},
            group_var='custom_group'
        )

        assert result['custom_group'].tolist() == ['combined', 'combined', 'combined']

    def test_remap_device_groups_preserves_other_columns(self):
        """Test that other columns are preserved during remapping."""
        df = pd.DataFrame({
            'search_group': ['A', 'B'],
            'id': [1, 2],
            'value': ['x', 'y'],
            'count': [10, 20]
        })

        result = analysis_helpers.remap_device_groups(df, new_group_mapping={
            'new-A': 'A',
            'new-B': 'B'
        })

        assert 'id' in result.columns
        assert 'value' in result.columns
        assert 'count' in result.columns
        assert result['value'].tolist() == ['x', 'y']
        assert result['count'].tolist() == [10, 20]


    def test_hierarchical_brand_standardization_all_levels(self):
        """Test hierarchical standardization with all three levels."""
        df = pd.DataFrame({
            'BRAND_NAME': [
                'Inari Medical ClotTriever XL',
                'Inari Medical ClotTriever BOLD',
                'CLOTTRIEVER CATHETER',  # Should match family but not specific
                'FlowTriever System',
                'Some Unknown Device'
            ],
            'MANUFACTURER_D_NAME': [
                'Inari Medical ClotTriever',
                'Inari Medical ClotTriever',
                'Inari Medical ClotTriever',
                'Inari Medical FlowTriever',
                'Unknown Manufacturer'
            ]
        })

        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
            'clottriever bold': 'Inari Medical ClotTriever BOLD',
        }
        family = {
            'clottriever': 'Inari Medical ClotTriever (unspecified)',
            'flowtriever': 'Inari Medical FlowTriever (unspecified)',
        }
        manufacturer = {
            'clottriever': 'Inari Medical',
            'flowtriever': 'Inari Medical',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df,
            specific_mapping=specific,
            family_mapping=family,
            manufacturer_mapping=manufacturer
        )

        # Check columns exist
        assert 'device_model' in result.columns
        assert 'device_family' in result.columns
        assert 'manufacturer' in result.columns

        # Check specific matches
        assert result.iloc[0]['device_model'] == 'Inari Medical ClotTriever XL'
        assert result.iloc[1]['device_model'] == 'Inari Medical ClotTriever BOLD'

        # Check family match (no specific match)
        assert result.iloc[2]['device_model'] == 'Inari Medical ClotTriever (unspecified)'
        assert result.iloc[2]['device_family'] == 'Inari Medical ClotTriever (unspecified)'

        # Check FlowTriever
        assert result.iloc[3]['device_model'] == 'Inari Medical FlowTriever (unspecified)'
        assert result.iloc[3]['manufacturer'] == 'Inari Medical'

        # Check unmatched device
        assert pd.isna(result.iloc[4]['device_model'])
        assert pd.isna(result.iloc[4]['manufacturer'])

    def test_hierarchical_brand_standardization_specific_only(self):
        """Test hierarchical standardization with only specific mapping."""
        df = pd.DataFrame({
            'BRAND_NAME': ['ClotTriever XL', 'ClotTriever BOLD', 'ClotTriever']
        })

        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
            'clottriever bold': 'Inari Medical ClotTriever BOLD',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, specific_mapping=specific
        )

        # First two should match
        assert result.iloc[0]['device_model'] == 'Inari Medical ClotTriever XL'
        assert result.iloc[1]['device_model'] == 'Inari Medical ClotTriever BOLD'

        # Third should not match (no family mapping provided)
        assert pd.isna(result.iloc[2]['device_model'])

    def test_hierarchical_brand_standardization_manufacturer_only(self):
        """Test hierarchical standardization with only manufacturer mapping."""
        df = pd.DataFrame({
            'BRAND_NAME': ['ClotTriever XL', 'FlowTriever', 'Lightning Bolt'],
            'MANUFACTURER_D_NAME': ['Inari ClotTriever', 'Inari FlowTriever', 'Penumbra Lightning']
        })

        manufacturer = {
            'clottriever': 'Inari Medical',
            'flowtriever': 'Inari Medical',
            'lightning': 'Penumbra',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, manufacturer_mapping=manufacturer
        )

        # All should have manufacturers but no device models
        assert result.iloc[0]['manufacturer'] == 'Inari Medical'
        assert result.iloc[1]['manufacturer'] == 'Inari Medical'
        assert result.iloc[2]['manufacturer'] == 'Penumbra'
        assert all(pd.isna(result['device_model']))

    def test_hierarchical_brand_standardization_prevents_double_match(self):
        """Test that specific matches prevent family matches."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Inari Medical ClotTriever XL Catheter']
        })

        # Both patterns would match, but specific should take precedence
        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
        }
        family = {
            'clottriever': 'Inari Medical ClotTriever (unspecified)',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df,
            specific_mapping=specific,
            family_mapping=family
        )

        # Should match specific, not family
        assert result.iloc[0]['device_model'] == 'Inari Medical ClotTriever XL'
        # Family should also be set since the brand name matches family pattern
        assert result.iloc[0]['device_family'] == 'Inari Medical ClotTriever (unspecified)'

    def test_hierarchical_brand_standardization_none_values(self):
        """Test hierarchical standardization handles None values."""
        df = pd.DataFrame({
            'BRAND_NAME': ['ClotTriever XL', None, 'FlowTriever']
        })

        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, specific_mapping=specific
        )

        # None should remain unmatched
        assert pd.isna(result.iloc[1]['device_model'])
        assert pd.isna(result.iloc[1]['manufacturer'])

    def test_hierarchical_brand_standardization_case_insensitive(self):
        """Test that matching is case-insensitive."""
        df = pd.DataFrame({
            'BRAND_NAME': ['CLOTTRIEVER XL', 'ClotTriever BOLD', 'clottriever']
        })

        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
            'clottriever bold': 'Inari Medical ClotTriever BOLD',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, specific_mapping=specific
        )

        assert result.iloc[0]['device_model'] == 'Inari Medical ClotTriever XL'
        assert result.iloc[1]['device_model'] == 'Inari Medical ClotTriever BOLD'

    def test_hierarchical_brand_standardization_missing_column(self):
        """Test error when source column missing."""
        df = pd.DataFrame({'OTHER_COLUMN': ['Device A']})

        with pytest.raises(ValueError, match="must contain 'BRAND_NAME' column"):
            analysis_helpers.hierarchical_brand_standardization(df, manufacturer_mapping={})

    def test_hierarchical_brand_standardization_custom_source_col(self):
        """Test hierarchical standardization with custom source column."""
        df = pd.DataFrame({
            'custom_brand': ['ClotTriever XL', 'FlowTriever'],
            'MANUFACTURER_D_NAME': ['Inari ClotTriever', 'Inari FlowTriever']
        })

        manufacturer = {
            'clottriever': 'Inari Medical',
            'flowtriever': 'Inari Medical',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, manufacturer_mapping=manufacturer, source_col='custom_brand'
        )

        assert result.iloc[0]['manufacturer'] == 'Inari Medical'
        assert result.iloc[1]['manufacturer'] == 'Inari Medical'

    def test_hierarchical_brand_standardization_preserves_original(self):
        """Test that original BRAND_NAME column is preserved."""
        df = pd.DataFrame({
            'BRAND_NAME': ['ClotTriever XL Original Name']
        })

        specific = {
            'clottriever xl': 'Inari Medical ClotTriever XL',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, specific_mapping=specific
        )

        # Original should be preserved
        assert result['BRAND_NAME'].iloc[0] == 'ClotTriever XL Original Name'
        # But standardized version should be in device_model
        assert result['device_model'].iloc[0] == 'Inari Medical ClotTriever XL'

    def test_hierarchical_brand_standardization_order_matters(self):
        """Test that more specific patterns should be listed first in mappings."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Lightning Bolt 7', 'Lightning Flash']
        })

        # If order matters, Lightning Bolt should match before Lightning
        family = {
            'lightning bolt': 'Penumbra Lightning Bolt (unspecified)',
            'lightning flash': 'Penumbra Lightning Flash (unspecified)',
            'lightning': 'Penumbra Lightning (unspecified)',
        }

        result = analysis_helpers.hierarchical_brand_standardization(
            df, family_mapping=family
        )

        # Should match the more specific patterns first
        assert result.iloc[0]['device_model'] == 'Penumbra Lightning Bolt (unspecified)'
        assert result.iloc[1]['device_model'] == 'Penumbra Lightning Flash (unspecified)'

    def test_summarize_by_brand(self):
        """Test brand summarization with search_group column (new default)."""
        df = pd.DataFrame({
            'search_group': ['Brand A', 'Brand A', 'Brand B', 'Brand B', 'Brand C'],
            'DATE_RECEIVED': ['2020-01-01', '2020-06-01', '2020-03-01', '2020-09-01', '2021-01-01'],
            'EVENT_TYPE': ['Injury', 'Death', 'Malfunction', 'Injury', 'Death']
        })

        summary = analysis_helpers.summarize_by_brand(df)

        assert summary['counts'] == {'Brand A': 2, 'Brand B': 2, 'Brand C': 1}
        assert 'temporal' in summary
        assert 'event_types' in summary
        assert 'date_range' in summary

    def test_summarize_by_brand_missing_column(self):
        """Test summarize_by_brand with missing group column."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Brand A', 'Brand B']
        })

        with pytest.raises(ValueError, match="must contain 'search_group' column"):
            analysis_helpers.summarize_by_brand(df)

    def test_create_contingency_table(self):
        """Test contingency table creation."""
        df = pd.DataFrame({
            'brand': ['A', 'A', 'B', 'B', 'C', 'C'],
            'category': ['Cat1', 'Cat2', 'Cat1', 'Cat1', 'Cat2', 'Cat2']
        })

        table = analysis_helpers.create_contingency_table(df, 'brand', 'category')

        assert table.loc['A', 'Cat1'] == 1
        assert table.loc['A', 'Cat2'] == 1
        assert table.loc['B', 'Cat1'] == 2

    def test_create_contingency_table_normalized(self):
        """Test contingency table with normalization."""
        df = pd.DataFrame({
            'brand': ['A', 'A', 'B', 'B', 'B', 'B'],
            'category': ['Cat1', 'Cat2', 'Cat1', 'Cat1', 'Cat1', 'Cat2']
        })

        result = analysis_helpers.create_contingency_table(df, 'brand', 'category', normalize=True)

        assert 'counts' in result
        assert 'percentages' in result
        # Brand A: 50% Cat1, 50% Cat2
        assert abs(result['percentages'].loc['A', 'Cat1'] - 50.0) < 0.1
        assert abs(result['percentages'].loc['A', 'Cat2'] - 50.0) < 0.1
        # Brand B: 75% Cat1, 25% Cat2
        assert abs(result['percentages'].loc['B', 'Cat1'] - 75.0) < 0.1
        assert abs(result['percentages'].loc['B', 'Cat2'] - 25.0) < 0.1

    def test_chi_square_test(self):
        """Test chi-square test."""
        # Create data with clear association
        df = pd.DataFrame({
            'brand': ['A']*10 + ['B']*10,
            'problem': ['Problem1']*9 + ['Problem2']*1 + ['Problem2']*9 + ['Problem1']*1
        })

        result = analysis_helpers.chi_square_test(df, 'brand', 'problem')

        assert 'chi2_statistic' in result
        assert 'p_value' in result
        assert 'dof' in result
        assert 'expected_frequencies' in result
        assert 'significant' in result
        assert result['significant'] == True  # Should be significant

    def test_event_type_comparison(self):
        """Test event type comparison with search_group column (new default)."""
        df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3, 4, 5, 6],
            'search_group': ['Brand A', 'Brand A', 'Brand A', 'Brand B', 'Brand B', 'Brand B'],
            'EVENT_TYPE': ['Death', 'Injury', 'Malfunction', 'Death', 'Death', 'Injury']
        })

        comparison = analysis_helpers.event_type_comparison(df)

        assert 'counts' in comparison
        assert 'percentages' in comparison
        assert 'chi2_test' in comparison
        assert 'summary' in comparison
        assert isinstance(comparison['summary'], str)
        assert 'Brand A' in comparison['summary']
        assert 'Brand B' in comparison['summary']

    def test_count_unique_outcomes_preserves_search_group(self):
        """Verify search_group column is preserved in output."""
        df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 1, 2, 2],
            'SEQUENCE_NUMBER_OUTCOME': ['D', 'H', 'IN', 'L'],
            'search_group': ['pump_a', 'pump_a', 'pump_b', 'pump_b']
        })
        result = analysis_helpers.count_unique_outcomes_per_report(df)

        assert 'search_group' in result.columns, "search_group column should be preserved"
        assert len(result) == 2  # One row per MDR_REPORT_KEY
        assert result[result['MDR_REPORT_KEY'] == 1]['search_group'].iloc[0] == 'pump_a'
        assert result[result['MDR_REPORT_KEY'] == 2]['search_group'].iloc[0] == 'pump_b'

    def test_count_unique_outcomes_preserves_multiple_columns(self):
        """Verify multiple consistent columns are preserved."""
        df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 1, 2],
            'SEQUENCE_NUMBER_OUTCOME': ['D', 'H', 'IN'],
            'search_group': ['pump_a', 'pump_a', 'pump_b'],
            'BRAND_NAME': ['Brand X', 'Brand X', 'Brand Y'],
            'EVENT_TYPE': ['Death', 'Death', 'Injury']
        })
        result = analysis_helpers.count_unique_outcomes_per_report(df)

        # All consistent columns should be preserved
        assert 'search_group' in result.columns
        assert 'BRAND_NAME' in result.columns
        assert 'EVENT_TYPE' in result.columns

    # ==================== exclude_results tests ====================

    def test_exclude_results_basic(self):
        """Test basic exclusion functionality."""
        main_df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3, 4, 5],
            'BRAND_NAME': ['A', 'B', 'C', 'D', 'E']
        })
        exclude_df = pd.DataFrame({
            'MDR_REPORT_KEY': [2, 4],
            'BRAND_NAME': ['B', 'D']
        })

        result = analysis_helpers.exclude_results(main_df, exclude_df)

        assert len(result) == 3
        assert list(result['MDR_REPORT_KEY']) == [1, 3, 5]

    def test_exclude_results_custom_key(self):
        """Test exclusion with custom key column."""
        main_df = pd.DataFrame({
            'EVENT_KEY': ['e1', 'e2', 'e3'],
            'DATA': ['x', 'y', 'z']
        })
        exclude_df = pd.DataFrame({
            'EVENT_KEY': ['e2'],
            'DATA': ['y']
        })

        result = analysis_helpers.exclude_results(main_df, exclude_df, key='EVENT_KEY')

        assert len(result) == 2
        assert list(result['EVENT_KEY']) == ['e1', 'e3']

    def test_exclude_results_empty_exclude(self):
        """Test with empty exclude DataFrame."""
        main_df = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3],
            'DATA': ['a', 'b', 'c']
        })
        exclude_df = pd.DataFrame({'MDR_REPORT_KEY': []})

        result = analysis_helpers.exclude_results(main_df, exclude_df)

        assert len(result) == 3

    def test_exclude_results_missing_key_main_raises(self):
        """Test that missing key in main_df raises ValueError."""
        main_df = pd.DataFrame({'OTHER_COL': [1, 2, 3]})
        exclude_df = pd.DataFrame({'MDR_REPORT_KEY': [1]})

        with pytest.raises(ValueError, match="main_df must contain"):
            analysis_helpers.exclude_results(main_df, exclude_df)

    def test_exclude_results_missing_key_exclude_raises(self):
        """Test that missing key in exclude_df raises ValueError."""
        main_df = pd.DataFrame({'MDR_REPORT_KEY': [1, 2, 3]})
        exclude_df = pd.DataFrame({'OTHER_COL': [1]})

        with pytest.raises(ValueError, match="exclude_df must contain"):
            analysis_helpers.exclude_results(main_df, exclude_df)

    # ==================== filter_by_text tests ====================

    def test_filter_by_text_exclude_terms(self):
        """Test filtering with exclude_terms."""
        df = pd.DataFrame({
            'DEVICE_NAME_CONCAT': [
                'INSULIN PUMP MODEL A',
                'THROMBECTOMY CATHETER',
                'INSULIN DELIVERY SYSTEM',
                'ANGIOJET DEVICE'
            ],
            'MDR_REPORT_KEY': [1, 2, 3, 4]
        })

        result = analysis_helpers.filter_by_text(df, exclude_terms=['insulin'])

        assert len(result) == 2
        assert list(result['MDR_REPORT_KEY']) == [2, 4]

    def test_filter_by_text_include_terms(self):
        """Test filtering with include_terms."""
        df = pd.DataFrame({
            'DEVICE_NAME_CONCAT': [
                'INSULIN PUMP',
                'THROMBECTOMY CATHETER',
                'CATHETER SYSTEM',
                'ANGIOJET'
            ],
            'MDR_REPORT_KEY': [1, 2, 3, 4]
        })

        result = analysis_helpers.filter_by_text(df, include_terms=['catheter'])

        assert len(result) == 2
        assert list(result['MDR_REPORT_KEY']) == [2, 3]

    def test_filter_by_text_both_exclude_and_include(self):
        """Test filtering with both exclude and include terms."""
        df = pd.DataFrame({
            'DEVICE_NAME_CONCAT': [
                'CATHETER A',
                'CATHETER B INSULIN',
                'PUMP',
                'CATHETER C'
            ],
            'MDR_REPORT_KEY': [1, 2, 3, 4]
        })

        result = analysis_helpers.filter_by_text(
            df,
            exclude_terms=['insulin'],
            include_terms=['catheter']
        )

        assert len(result) == 2
        assert list(result['MDR_REPORT_KEY']) == [1, 4]

    def test_filter_by_text_custom_column(self):
        """Test filtering with custom column."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Omni Pump', 'Angiojet', 'Omni Cleaner'],
            'MDR_REPORT_KEY': [1, 2, 3]
        })

        result = analysis_helpers.filter_by_text(
            df,
            include_terms=['omni'],
            column='BRAND_NAME'
        )

        assert len(result) == 2
        assert list(result['MDR_REPORT_KEY']) == [1, 3]

    def test_filter_by_text_missing_column_raises(self):
        """Test that missing column raises ValueError."""
        df = pd.DataFrame({'OTHER_COL': ['a', 'b', 'c']})

        with pytest.raises(ValueError, match="must contain"):
            analysis_helpers.filter_by_text(df, exclude_terms=['test'])

    def test_filter_by_text_case_insensitive(self):
        """Test that filtering is case insensitive."""
        df = pd.DataFrame({
            'DEVICE_NAME_CONCAT': ['INSULIN', 'insulin', 'Insulin'],
            'MDR_REPORT_KEY': [1, 2, 3]
        })

        result = analysis_helpers.filter_by_text(df, exclude_terms=['INSULIN'])

        assert len(result) == 0

    # ==================== summarize_devices tests ====================

    def test_summarize_devices_basic(self):
        """Test basic device summarization."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Brand A', 'Brand A', 'Brand B'],
            'GENERIC_NAME': ['Generic 1', 'Generic 1', 'Generic 2'],
            'MANUFACTURER_D_NAME': ['Mfr X', 'Mfr X', 'Mfr Y'],
            'MDR_REPORT_KEY': [1, 2, 3]
        })

        result = analysis_helpers.summarize_devices(df)

        assert len(result) == 2  # Two unique device combinations
        assert 'BRAND_NAME' in result.columns
        assert 'GENERIC_NAME' in result.columns
        assert 'MANUFACTURER_D_NAME' in result.columns

    def test_summarize_devices_custom_columns(self):
        """Test summarization with custom columns."""
        df = pd.DataFrame({
            'BRAND_NAME': ['A', 'A', 'B'],
            'GENERIC_NAME': ['G1', 'G1', 'G2'],
            'MDR_REPORT_KEY': [1, 2, 3]
        })

        result = analysis_helpers.summarize_devices(df, columns=['BRAND_NAME'])

        assert len(result) == 2
        assert list(result.columns) == ['BRAND_NAME']

    def test_summarize_devices_missing_optional_columns(self):
        """Test that missing optional columns are gracefully handled."""
        df = pd.DataFrame({
            'BRAND_NAME': ['A', 'B', 'A'],
            'MDR_REPORT_KEY': [1, 2, 3]
        })

        # Default columns include MANUFACTURER_D_NAME which is missing
        result = analysis_helpers.summarize_devices(df)

        assert len(result) == 2
        assert 'BRAND_NAME' in result.columns
        assert 'MANUFACTURER_D_NAME' not in result.columns

    def test_summarize_devices_no_valid_columns_raises(self):
        """Test that no valid columns raises ValueError."""
        df = pd.DataFrame({
            'OTHER_COL': ['a', 'b', 'c']
        })

        with pytest.raises(ValueError, match="None of the specified columns"):
            analysis_helpers.summarize_devices(df)

    def test_summarize_devices_sorted_output(self):
        """Test that output is sorted by first column."""
        df = pd.DataFrame({
            'BRAND_NAME': ['Zebra', 'Apple', 'Middle'],
            'GENERIC_NAME': ['G1', 'G2', 'G3']
        })

        result = analysis_helpers.summarize_devices(df)

        assert result['BRAND_NAME'].iloc[0] == 'Apple'
        assert result['BRAND_NAME'].iloc[2] == 'Zebra'


@pytest.mark.integration
class TestAnalysisHelpersIntegration:
    """Integration tests with real test database."""

    @pytest.fixture
    def db_with_data(self, tmp_path):
        """Create test database with sample data."""
        db_path = str(tmp_path / 'test.db')
        db = MaudeDatabase(db_path, verbose=False)

        # Create sample device data
        device_data = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3, 4, 5],
            'BRAND_NAME': ['Test Device A', 'Test Device B', 'Test Device A', 'Test Device C', 'Test Device B'],
            'GENERIC_NAME': ['Device Type 1', 'Device Type 2', 'Device Type 1', 'Device Type 3', 'Device Type 2'],
            'MANUFACTURER_D_NAME': ['Acme Corp', 'Beta Inc', 'Acme Corp', 'Gamma LLC', 'Beta Inc'],
            'DEVICE_REPORT_PRODUCT_CODE': ['ABC', 'DEF', 'ABC', 'GHI', 'DEF'],
            'DATE_RECEIVED': ['2020-01-15', '2020-06-20', '2021-03-10', '2021-08-05', '2021-12-25'],
            'EVENT_TYPE': ['Death', 'Injury', 'Malfunction', 'Death', 'Injury']
        })

        # Insert into database
        device_data.to_sql('device', db.conn, if_exists='replace', index=False)

        # Create master table entry with EVENT_KEY column
        master_data = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2, 3, 4, 5],
            'EVENT_KEY': ['EVT1', 'EVT2', 'EVT3', 'EVT4', 'EVT5'],
            'DATE_RECEIVED': ['2020-01-15', '2020-06-20', '2021-03-10', '2021-08-05', '2021-12-25'],
            'EVENT_TYPE': ['Death', 'Injury', 'Malfunction', 'Death', 'Injury']
        })
        master_data.to_sql('master', db.conn, if_exists='replace', index=False)

        yield db
        db.close()

    def test_enrich_missing_table_raises_error(self, db_with_data):
        """Test strict error when table not loaded."""
        df = pd.DataFrame({'MDR_REPORT_KEY': [1, 2, 3]})

        with pytest.raises(ValueError, match="Problems table not loaded"):
            analysis_helpers.enrich_with_problems(db_with_data, df)

        with pytest.raises(ValueError, match="Patient table not loaded"):
            analysis_helpers.enrich_with_patient_data(db_with_data, df)

        with pytest.raises(ValueError, match="Text table not loaded"):
            analysis_helpers.enrich_with_narratives(db_with_data, df)

    def test_get_narratives_for_wrapper(self, db_with_data):
        """Test get_narratives_for with database instance."""
        # Create text table
        text_data = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2],
            'FOI_TEXT': ['Event narrative 1', 'Event narrative 2']
        })
        text_data.to_sql('text', db_with_data.conn, if_exists='replace', index=False)

        results = pd.DataFrame({'MDR_REPORT_KEY': [1, 2, 3]})
        narratives = analysis_helpers.get_narratives_for(db_with_data, results)

        assert len(narratives) == 2  # Only 2 have narratives
        assert 'FOI_TEXT' in narratives.columns

    def test_backwards_compatibility_via_db_instance(self, db_with_data):
        """Test that query methods work through database instance."""
        # Use exact-match query with new API
        results = db_with_data.query_device(brand_name='Test Device A')

        # Test old helper methods
        trends = db_with_data.trends_for(results)
        assert len(trends) > 0

        breakdown = db_with_data.event_type_breakdown_for(results)
        assert 'total' in breakdown

        top_mfg = db_with_data.top_manufacturers_for(results)
        assert len(top_mfg) > 0

        date_summary = db_with_data.date_range_summary_for(results)
        assert 'first_date' in date_summary

    def test_new_methods_via_db_instance(self, db_with_data):
        """Test that helper methods work through database instance."""
        # Use search_by_device_names instead of old query_device with device_name
        results = db_with_data.search_by_device_names('Test Device A')
        assert len(results) > 0

        # Test standardize_brand_names
        mapping = {'test device a': 'Device A', 'test device b': 'Device B'}
        standardized = db_with_data.standardize_brand_names(results, mapping)
        assert 'standard_brand' in standardized.columns

        # Test summarize_by_brand with custom group_column
        summary = db_with_data.summarize_by_brand(standardized, group_column='standard_brand')
        assert 'counts' in summary

    def test_hierarchical_brand_standardization_via_db_instance(self, db_with_data):
        """Test hierarchical standardization through database instance."""
        # Use search_by_device_names instead of old query_device with device_name
        results = db_with_data.search_by_device_names('Test Device')

        specific = {
            'test device a': 'Test Device A (Specific)',
        }
        family = {
            'test device': 'Test Device (Generic)',
        }
        manufacturer = {
            'test device': 'Test Manufacturer',
        }

        result = db_with_data.hierarchical_brand_standardization(
            results,
            specific_mapping=specific,
            family_mapping=family,
            manufacturer_mapping=manufacturer
        )

        assert 'device_model' in result.columns
        assert 'device_family' in result.columns
        assert 'manufacturer' in result.columns
        assert len(result) == len(results)  # Should preserve all rows

    def test_enrich_with_patient_data_handles_many_keys(self, db_with_data):
        """Verify enrichment works with key count that could exceed SQLite limit."""
        # Create patient table with 25 records (enough to test batching with small batch size)
        patient_data = pd.DataFrame({
            'MDR_REPORT_KEY': list(range(1, 26)),
            'SEQUENCE_NUMBER_OUTCOME': ['D'] * 10 + ['IN'] * 10 + ['H'] * 5
        })
        patient_data.to_sql('patient', db_with_data.conn, if_exists='replace', index=False)

        # Create input DataFrame with 25 keys
        results_df = pd.DataFrame({'MDR_REPORT_KEY': list(range(1, 26))})

        # This should work without hitting SQLite limit (uses batching internally)
        enriched = analysis_helpers.enrich_with_patient_data(db_with_data, results_df)

        assert len(enriched) == 25
        assert 'SEQUENCE_NUMBER_OUTCOME' in enriched.columns

    def test_enrich_with_patient_data_empty_input(self, db_with_data):
        """Verify enrichment handles empty DataFrame gracefully."""
        patient_data = pd.DataFrame({
            'MDR_REPORT_KEY': [1, 2],
            'SEQUENCE_NUMBER_OUTCOME': ['D', 'IN']
        })
        patient_data.to_sql('patient', db_with_data.conn, if_exists='replace', index=False)

        results_df = pd.DataFrame({'MDR_REPORT_KEY': pd.Series([], dtype=int)})
        enriched = analysis_helpers.enrich_with_patient_data(db_with_data, results_df)

        assert len(enriched) == 0

    def test_enrich_with_narratives_handles_many_keys(self, db_with_data):
        """Verify narrative enrichment works with many keys."""
        # Create text table with 25 records
        text_data = pd.DataFrame({
            'MDR_REPORT_KEY': list(range(1, 26)),
            'FOI_TEXT': [f'Narrative text {i}' for i in range(1, 26)]
        })
        text_data.to_sql('text', db_with_data.conn, if_exists='replace', index=False)

        # Create input DataFrame with 25 keys
        results_df = pd.DataFrame({'MDR_REPORT_KEY': list(range(1, 26))})

        # This should work without hitting SQLite limit
        enriched = analysis_helpers.enrich_with_narratives(db_with_data, results_df)

        assert len(enriched) == 25
        assert 'FOI_TEXT' in enriched.columns

    def test_enrich_with_problems_handles_many_keys(self, db_with_data):
        """Verify problems enrichment works with many keys."""
        # Create problems table with 25 records
        problems_data = pd.DataFrame({
            'MDR_REPORT_KEY': list(range(1, 26)),
            'DEVICE_SEQUENCE_NUMBER': [1] * 25,
            'DEVICE_PROBLEM_CODE': ['1234'] * 25
        })
        problems_data.to_sql('problems', db_with_data.conn, if_exists='replace', index=False)

        # Create input DataFrame with 25 keys
        results_df = pd.DataFrame({'MDR_REPORT_KEY': list(range(1, 26))})

        # This should work without hitting SQLite limit
        enriched = analysis_helpers.enrich_with_problems(db_with_data, results_df)

        assert len(enriched) == 25
        assert 'DEVICE_PROBLEM_CODE' in enriched.columns

    def test_get_narratives_handles_many_keys(self, db_with_data):
        """Verify get_narratives works with many keys."""
        # Create text table with 25 records
        text_data = pd.DataFrame({
            'MDR_REPORT_KEY': list(range(1, 26)),
            'FOI_TEXT': [f'Narrative text {i}' for i in range(1, 26)]
        })
        text_data.to_sql('text', db_with_data.conn, if_exists='replace', index=False)

        # This should work without hitting SQLite limit
        narratives = db_with_data.get_narratives(list(range(1, 26)))

        assert len(narratives) == 25
        assert 'FOI_TEXT' in narratives.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
