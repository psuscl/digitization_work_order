ArchivesSpace::Application.routes.draw do

  [AppConfig[:frontend_proxy_prefix], AppConfig[:frontend_prefix]].uniq.each do |prefix|

    scope prefix do
      match('/plugins/work_order/generate_report' => 'work_order#generate_report', :via => [:post])
      match('/plugins/work_order/generate_ladybird_export' => 'work_order#generate_ladybird_export', :via => [:post])
      match('/plugins/work_order/generate_goobi_export' => 'work_order#generate_goobi_export', :via => [:post])
      match('/plugins/work_order/generate_psul_export' => 'work_order#generate_psul_export', :via => [:post])
      match('/plugins/work_order' => 'work_order#index', :via => [:get])
    end
  end
end
