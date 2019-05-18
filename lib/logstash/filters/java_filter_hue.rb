# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash-filter-java_filter_hue_jars"
require "java"

class LogStash::Filters::JavaFilterHue < LogStash::Filters::Base
  config_name "java_filter_hue"

  def self.javaClass() org.logstash.javaapi.JavaFilterHue.java_class; end
end