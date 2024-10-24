require 'write_xlsx'

class PsulExport

  def column_definitions
    [
      {:header => "title", :proc => Proc.new {|row| title(row)}},
      {:header => "archival object", :proc => Proc.new{|row| local_record_id(row)}},
      {:header => "collection", :proc => Proc.new {|row| resource_title(row)}},
      {:header => "dates", :proc => Proc.new {|row| date_string(row)}},
      {:header => "finding aid", :proc => Proc.new {|row| ead_location(row)}},
      {:header => "identifier", :proc => Proc.new {|row| digital_object_identifier(row)}},
      {:header => "series", :proc => Proc.new {|row| series_title(row)}},
      {:header => "subseries", :proc => Proc.new {|row| subseries_title(row)}},
      {:header => "container information", :proc => Proc.new {|row| container_information(row)}},
      {:header => "file url", :proc => Proc.new {|row| file_uri(row)}},
    ]
  end

  def initialize(uris, resource_uri)
    @uris = uris
    @resource = resource_uri
    @ids = extract_ids
  end

  def to_stream
    io = StringIO.new
    wb = WriteXLSX.new(io)

    sheet = wb.add_worksheet('Digitization Work Order')

    row_ix = 0
    sheet.write_row(row_ix, 0, column_definitions.collect{|col| col.fetch(:header)})

    dataset.all.sort{|x,y| @ids.index(x[:archival_object_id]) <=> @ids.index(y[:archival_object_id])}.each do |row|
      row_ix += 1
      sheet.write_row(row_ix, 0, column_definitions.map {|col| col[:proc].call(row) })
    end

    wb.close
    io.string
  end

  def series_for_object(id)
    @series.fetch(id, [])
           .collect{|ao| strip_html(ao.fetch('title'))}
           .join('.')
  end

  def subseries_for_object(id)
    @subseries.fetch(id, [])
              .collect{|ao| strip_html(ao.fetch('title'))}
              .join('.')
  end

  def container_labels_for_archival_object(id)
    @container_labels.fetch(id, [])
  end

  def file_versions_for_archival_object(id)
    @file_versions.fetch(id, [])
  end

  def date_string_for_archival_object(id)
    @dates.fetch(id, [])
  end


  private


  def dataset
    ds = ArchivalObject
           .left_outer_join(:resource, :resource__id => :archival_object__root_record_id)
           .left_outer_join(:repository, :repository__id => :archival_object__repo_id)
           .filter(:archival_object__id => @ids)

    # archival object bits
    ds = ds.select_append(Sequel.as(:archival_object__id, :archival_object_id))
    ds = ds.select_append(Sequel.as(:archival_object__title, :archival_object_title))
    ds = ds.select_append(Sequel.as(:archival_object__ref_id, :identifier))

    # resource bits
    ds = ds.select_append(Sequel.as(:resource__id, :resource_id))
    ds = ds.select_append(Sequel.as(:resource__title, :resource_title))
    ds = ds.select_append(Sequel.as(:resource__ead_id, :resource_ead_id))
    ds = ds.select_append(Sequel.as(:resource__ead_location, :resource_ead_location))

    # repository bits
    ds = ds.select_append(Sequel.as(:repository__repo_code, :repository_code))

    prepare_breadcrumbs
    prepare_container_labels
    prepare_file_versions
    prepare_dates

    ds
  end

  def extract_ids
    @uris.map { |uri|
      parsed = JSONModel.parse_reference(uri)

      # only archival_objects
      next unless parsed[:type] == "archival_object"

      parsed[:id]
    }.compact
  end

  def title(row)
    strip_html(row[:archival_object_title])
  end

  def local_record_id(row)
    "/repositories/#{row[:repo_id]}/archival_objects/#{row[:archival_object_id]}"
  end
  
  def resource_title(row)
    strip_html(row[:resource_title])
  end

  def ead_location(row)
    strip_html(row[:resource_ead_location])
  end

  def digital_object_identifier(row)
    [row[:repository_code], row[:resource_ead_id], row[:identifier]].join('_')
  end

  def series_title(row)
    series_for_object(row[:archival_object_id])
  end

  def subseries_title(row)
    subseries_for_object(row[:archival_object_id])
  end

  def container_information(row)
    container_labels_for_archival_object(row[:archival_object_id]).map{|row|
      if row[:sub_container]
        [row[:top_container], row[:sub_container]].compact.join(', ')
      else
        row[:top_container]
      end
    }
  end
  
  def file_uri(row)
    file_versions_for_archival_object(row[:archival_object_id]).map{|row| row[:file_uri]}
  end

  def date_string(row)
    date_string_for_archival_object(row[:archival_object_id]).map{|date|
      if date[:expression]
        dates = date[:expression]
      else
        dates = [date[:begin], date[:end]].compact.join('--')
      end
      "#{date[:label]}: #{dates}"
    }.join('; ')
  end

  def prepare_breadcrumbs
    child_to_parent_map = {}
    node_to_position_map = {}
    node_to_root_record_map = {}
    node_to_data_map = {}

    @series = {}
    @subseries = {}

    DB.open do |db|
      nodes_to_expand = @ids

      while !nodes_to_expand.empty?
        next_nodes_to_expand = []

        db[:archival_object]
          .left_outer_join(:enumeration_value, { :level_enum__id => :archival_object__level_id }, :table_alias => :level_enum)
          .filter(:archival_object__id => nodes_to_expand)
          .select(Sequel.as(:archival_object__id, :id),
                  Sequel.as(:archival_object__parent_id, :parent_id),
                  Sequel.as(:archival_object__root_record_id, :root_record_id),
                  Sequel.as(:archival_object__position, :position),
                  Sequel.as(:archival_object__title, :title),
                  Sequel.as(:archival_object__display_string, :display_string),
                  Sequel.as(:archival_object__component_id, :component_id),
                  Sequel.as(:level_enum__value, :level),
                  Sequel.as(:archival_object__other_level, :other_level))
          .each do |row|
          child_to_parent_map[row[:id]] = row[:parent_id]
          node_to_position_map[row[:id]] = row[:position]
          node_to_data_map[row[:id]] = row
          node_to_root_record_map[row[:id]] = row[:root_record_id]
          next_nodes_to_expand << row[:parent_id]
        end

        nodes_to_expand = next_nodes_to_expand.compact.uniq
      end

      @ids.each do |node_id|
        s = []
        ss = []

        current_node = node_id
        while child_to_parent_map[current_node]
          parent_node = child_to_parent_map[current_node]

          data = node_to_data_map.fetch(parent_node)

          obj = {"uri" => JSONModel::JSONModel(:archival_object).uri_for(parent_node, :repo_id => @repo_id),
                   "display_string" => data.fetch(:display_string),
                   "title" => data.fetch(:title),
                   "level" => data[:other_level] || data[:level]}
          
          if obj['level'] == "series"
            s << obj
          elsif obj['level'] == "subseries"
            ss << obj
          end

          current_node = parent_node
        end

        @series[node_id] = s.reverse
        @subseries[node_id] = ss.reverse
      end
    end
  end

  def prepare_container_labels
    @container_labels = {}

    TopContainer
      .left_outer_join(:top_container_link_rlshp, :top_container_link_rlshp__top_container_id => :top_container__id)
      .left_outer_join(:sub_container, :sub_container__id => :top_container_link_rlshp__sub_container_id)
      .left_outer_join(:instance, :instance__id => :sub_container__instance_id)
      .left_outer_join(:enumeration_value, { :top_container_type__id => :top_container__type_id }, :table_alias => :top_container_type)
      .left_outer_join(:enumeration_value, { :sub_container_type__id => :sub_container__type_2_id }, :table_alias => :sub_container_type)
      .filter(:instance__archival_object_id => @ids)
      .select(Sequel.as(:instance__archival_object_id, :archival_object_id),
              Sequel.join([:top_container_type__value, ' ', :top_container__indicator]).as(:top_container),
              Sequel.join([:sub_container_type__value, ' ', :sub_container__indicator_2]).as(:sub_container))
      .each do |row|
        @container_labels[row[:archival_object_id]] ||= []
        @container_labels[row[:archival_object_id]] << row
    end
  end

  def prepare_file_versions
    @file_versions = {}

    FileVersion
      .left_outer_join(:digital_object, :digital_object__id => :file_version__digital_object_id)
      .left_outer_join(:instance_do_link_rlshp, :instance_do_link_rlshp__digital_object_id => :digital_object__id)
      .left_outer_join(:instance, :instance__id => :instance_do_link_rlshp__instance_id)
      .left_outer_join(:enumeration_value, { :instance_type__id => :instance__instance_type_id }, :table_alias => :instance_type)
      .filter(:instance_type__value => 'digital_object')
      .filter(:file_version__is_representative => true)
      .filter(:instance__archival_object_id => @ids)
      .select(Sequel.as(:instance__archival_object_id, :archival_object_id),
              Sequel.as(:file_version__file_uri, :file_uri))
      .each do |row|
        @file_versions[row[:archival_object_id]] ||= []
        @file_versions[row[:archival_object_id]] << row
    end
  end

  def prepare_dates
    @dates = {}

    ASDate
      .left_outer_join(:enumeration_value, {:date_label__id => :date__label_id}, :table_alias => :date_label)
      .filter(:date__archival_object_id => @ids)
      .select(Sequel.as(:date__archival_object_id, :archival_object_id),
              Sequel.as(:date_label__value, :label),
              Sequel.as(:date__begin, :begin),
              Sequel.as(:date__end, :end),
              Sequel.as(:date__expression, :expression))
        .each do |date|
        @dates[date[:archival_object_id]] ||= []
        @dates[date[:archival_object_id]] << date
    end
  end


  def strip_html(string)
    return if string.nil?

    string.gsub(/<\/?[^>]*>/, "")
  end

end
