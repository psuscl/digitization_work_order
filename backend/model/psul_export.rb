require 'write_xlsx'

class PsulExport

  NEW_LINE_SEPARATOR = ' | '

  def initialize(uris, resource_uri)
    @uris = uris
    @resource_uri = resource_uri
    @ids = extract_ids

    parsed_resource_uri = JSONModel.parse_reference(@resource_uri)
    @resource_id = parsed_resource_uri.fetch(:id)
    parsed_repo_uri = JSONModel.parse_reference(parsed_resource_uri.fetch(:repository))
    @repo_id = parsed_repo_uri.fetch(:id)
  end

  def column_definitions
    [
      # Title
      {:header => "title", :proc => Proc.new {|row| title(row)}},
      # Object URI
      {:header => "archival object", :proc => Proc.new{|row| local_record_id(row)}},
      # Date Created
      {:header => "date created", :proc => Proc.new {|row| creation_date(row)}},
      # Collection
      {:header => "collection", :proc => Proc.new {|row| resource_title(row)}},
      # Finding Aid
      {:header => "finding aid", :proc => Proc.new {|row| ead_location(row)}},
      # Identifier
      {:header => "identifier", :proc => Proc.new {|row| digital_object_identifier(row)}},
      # Container Information
      {:header => "container information", :proc => Proc.new{|row| container_information(row)}},
      # URL
      {:header => "url", :proc => Proc.new {|row| file_uri(row)}},
    ]
  end

  def to_stream
    io = StringIO.new
    wb = WriteXLSX.new(io)

    sheet = wb.add_worksheet('Digitization Work Order')

    hl_color = wb.set_custom_color(15, '#E8F4FF')
    highlight = wb.add_format(:bg_color => 15)

    row_ix = 0
    sheet.write_row(row_ix, 0, column_definitions.collect{|col| col.fetch(:header)})

    # PLEASE NOTE
    # `dataset` hits the database to return all the instance rows but it also
    # fire a series of extra queries from which we aggregate all multi-valued
    # fields required for the report. These values are stored as instance
    # variables and as such many of the helper methods will only return data
    # once `dataset` has been called.
    dataset.all.sort{|x,y| @ids.index(x[:archival_object_id]) <=> @ids.index(y[:archival_object_id])}.each do |row|
      row_ix += 1
      row_style = nil
      
      sheet.write_row(row_ix, 0, column_definitions.map {|col| col[:proc].call(row) }, row_style)
    end

    wb.close
    io.string
  end

  def container_labels_for_archival_object(id)
    @container_labels.fetch(id, [])
  end

  def file_versions_for_archival_object(id)
    @file_versions.fetch(id, [])
  end

  def creation_dates_for_archival_object(id)
    @creation_dates.fetch(id, [])
  end

  def all_dates_for_archival_object(id)
    @all_dates.fetch(id, [])
  end

  def extents_for_archival_object(id)
    @extents.fetch(id, [])
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

    prepare_ao_creation_dates
    prepare_container_labels
    prepare_file_versions

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

  def prepare_ao_creation_dates
    @all_dates = {}
    @creation_dates = {}

    creation_enum_id = EnumerationValue
                         .filter(:enumeration_id => Enumeration.filter(:name => 'date_label').select(:id))
                         .filter(:value => 'creation')
                         .select(:id)
                         .first[:id]

    ASDate
      .filter(:date__archival_object_id => @ids)
      .select(:archival_object_id,
              :expression,
              :begin,
              :end,
              Sequel.as(:date__date_type_id, :date_type_id),
              Sequel.as(:date__label_id, :label_id))
      .each do |row|
      @all_dates[row[:archival_object_id]] ||= []
      @all_dates[row[:archival_object_id]] << row

      if row[:label_id] == creation_enum_id
        @creation_dates[row[:archival_object_id]] ||= []
        @creation_dates[row[:archival_object_id]] << row
      end
    end
  end

  def prepare_extents
    @extents = {}

    Extent
     .left_outer_join(:enumeration_value, { :portion_enum__id => :extent__portion_id }, :table_alias => :portion_enum)
     .left_outer_join(:enumeration_value, { :extent_type_enum__id => :extent__extent_type_id }, :table_alias => :extent_type_enum)
     .filter(:extent__archival_object_id => @ids)
     .select(Sequel.as(:extent__archival_object_id, :archival_object_id),
             Sequel.as(:portion_enum__value, :portion),
             Sequel.as(:extent_type_enum__value, :extent_type),
             Sequel.as(:extent__number, :number))
     .each do |row|

      @extents[row[:archival_object_id]] ||= []
      @extents[row[:archival_object_id]] << row
    end
  end

  def local_record_id(row)
    "/repositories/#{row[:repo_id]}/archival_objects/#{row[:archival_object_id]}"
  end

  def digital_object_identifier(row)
    "#{row[:repository_code]}_#{row[:resource_ead_id]}_#{row[:identifier]}"
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

  def resource_title(row)
    strip_html(row[:resource_title])
  end

  def title(row)
    strip_html(row[:archival_object_title])
  end

  def creator(row)
    creators_for_archival_object(row[:archival_object_id])
      .map{|row| (row[:person] || row[:corporate_entity] || row[:family] || row[:software])}
      .join(NEW_LINE_SEPARATOR)
  end

  def creation_date(row)
    creation_dates_for_archival_object(row[:archival_object_id])
      .map{|row| [row[:begin], row[:end]].compact.join(' - ') || row[:expression]}
      .join(NEW_LINE_SEPARATOR)
  end

  def physical_description(row)
    extents_for_archival_object(row[:archival_object_id]).map{|row|
      type = I18n.t("enumerations.extent_extent_type.#{row[:extent_type]}",
                    :default => row[:extent_type])
      "#{row[:number]} #{type}"
    }.join(NEW_LINE_SEPARATOR)
  end

  def ead_location(row)
    row[:resource_ead_location]
  end

  def start_year(row)
    all_years(row, :start)
  end

  def end_year(row)
    all_years(row, :end)
  end

  def all_years(row, mode = :range)
    dates = all_dates_for_archival_object(row[:archival_object_id])

    return if dates.empty?

    ranges = []

    dates.each do |date|
      from = nil
      to = nil

      if date[:begin] && date[:begin] =~ /^[0-9][0-9][0-9][0-9]/
        year = date[:begin][0..3].to_i
        from = [from, year].compact.min
        to = year if to.nil?
      end

      if date[:end] && date[:end] =~ /^[0-9][0-9][0-9][0-9]/
        year = date[:end][0..3].to_i
        from = [from, year].compact.min
        to = [to, year].compact.max
      end

      next if from.nil?

      ranges << [from, to]
    end

    return if ranges.empty?

    full_range = ranges
      .collect{|r| (r[0]..r[1]).to_a}
      .flatten
      .uniq
      .sort

    case mode
    when :start
      full_range.first
    when :end
      full_range.last
    else
      full_range.join(NEW_LINE_SEPARATOR)
    end
  end

  def strip_html(string)
    return if string.nil?

    string.gsub(/<\/?[^>]*>/, "")
  end

end
