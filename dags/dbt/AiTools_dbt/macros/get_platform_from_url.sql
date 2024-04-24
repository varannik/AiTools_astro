


{% macro get_platform_from_url(url) %}



    {% set platforms %}
        SELECT *
        FROM {{ source ('api', 'core_platforms') }}
    {% endset %}

    {% set platforms_table = run_query(platforms) %}
    {% set found_platform = [] %}


    {% for plf in platforms_table %}

        {% if plf.base_domain in url %}
        {% set _ = found_substrings.append(plf.platform) %}
        {% endif %}
    {% endfor %}

    {% if found_substrings %}
        {{ return ~ found_substrings | join(', ') }}
    {% else %}
       ('WEB')
    {% endif %}

{% endmacro %}
