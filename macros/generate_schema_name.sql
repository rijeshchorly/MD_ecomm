{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'dev' -%}

      analytics_data_hub_{{ default_schema }}

    {%- elif target.name == 'pull_request' -%}

      {{ default_schema }}

    {%- else -%}

      {%- if custom_schema_name is none -%}

          {{ default_schema }}

      {%- else -%}

          {{ custom_schema_name | trim }}_{{ default_schema }}

      {%- endif -%}

    {%- endif -%}

{%- endmacro %}

