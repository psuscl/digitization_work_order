require 'cgi'

class WorkOrderController < ApplicationController

  set_access_control "view_repository" => [:index, :generate_report, :generate_ladybird_export, :generate_goobi_export, :generate_psul_export]

  def index
    @uri = params[:resource]
    @tree = escape_xml_characters(load_tree)
  end


  def generate_ladybird_export
    uri = "/plugins/digitization_work_order/repositories/#{session[:repo_id]}/ladybird"
    args = {'uri[]' => JSON.parse(params[:selected]), 'resource_uri' => params[:resource]}

    generate_response(uri, args)
  end

  def generate_goobi_export
    uri = "/plugins/digitization_work_order/repositories/#{session[:repo_id]}/goobi"
    args = {'uri[]' => JSON.parse(params[:selected]), 'resource_uri' => params[:resource]}

    generate_response(uri, args)
  end

  def generate_psul_export
    uri = "/plugins/digitization_work_order/repositories/#{session[:repo_id]}/psul"
    args = {'uri[]' => JSON.parse(params[:selected]), 'resource_uri' => params[:resource]}

    generate_response(uri, args)
  end

  def generate_report
    uri = "/plugins/digitization_work_order/repositories/#{session[:repo_id]}/report"
    args = {
      'uri[]' => JSON.parse(params[:selected]),
      'extras[]' => JSON.parse(params[:extras]),
      'generate_ids' => params[:report_type] == 'downloadWorkOrder'
    }

    generate_response(uri, args)
  end


  private


  def generate_response(uri, args)
    queue = Queue.new

    backend_session = JSONModel::HTTP::current_backend_session

    Thread.new do
      JSONModel::HTTP::current_backend_session = backend_session
      begin
        post_with_stream_response(uri, args) do |report_response|
          response.headers['Content-Disposition'] = report_response['Content-Disposition']
          response.headers['Content-Type'] = report_response['Content-Type']
          response.headers['Last-Modified'] = Time.now.to_s
          response.headers['Cache-Control'] = 'no-cache'
          response.headers['X-Content-Type-Options'] = 'nosniff'

          queue << :ok
          report_response.read_body do |chunk|
            queue << chunk unless chunk.empty?
          end
        end
      rescue
        queue << {:error => $!.message}
      ensure
        queue << :EOF
      end
    end

    first_on_queue = queue.pop # :ok or error hash
    if first_on_queue.kind_of?(Hash)
      @report_errors = first_on_queue[:error]

      @uri = params[:resource]
      @tree = escape_xml_characters(load_tree)

      return render :action => :index
    end

    self.response_body = Class.new do
      def self.queue=(queue)
        @queue = queue
      end
      def self.each(&block)
        while(true)
          chunk = @queue.pop

          break if chunk === :EOF

          block.call(chunk)
        end
      end
    end

    self.response_body.queue = queue

  end


  def escape_xml_characters(tree)
    result = tree.merge('title' => CGI.escapeHTML(tree['title']))

    if tree['children']
      result.merge('children' => tree['children'].map {|child| escape_xml_characters(child)})
    else
      result
    end
  end


  def load_tree
    JSONModel::HTTP::get_json(@uri + "/small_tree")
  end


  def post_with_stream_response(uri, params = {}, &block)
    uri = URI("#{ JSONModel::backend_url}#{uri}")

    req = Net::HTTP::Post.new(uri.request_uri)
    req.body = URI.encode_www_form(params)

    req['X-ArchivesSpace-Session'] = JSONModel::HTTP::current_backend_session

    Net::HTTP.start(uri.host, uri.port) do |http|
      http.request(req, nil) do |response|
        if response.code =~ /^4/
          #JSONModel::handle_error(ASUtils.json_parse(response.body))
          raise response.body
        end

        block.call(response)
      end
    end
  end

end
