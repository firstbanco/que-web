require "spec_helper"

describe Que::Web::Viewmodels::JobList do
  let(:job) {
    {"priority"=>100, "run_at"=> Time.now,
     "job_id"=>555, "job_class"=>"SuccessJob",
     "args"=>["arg1", {"name"=>"foo", "age"=>10}],
     "error_count"=>0,
     "last_error"=>nil,
     "queue"=>"foo"
    }
  }
  let(:pager) { Que::Web::Pager.new(1,10,105) }
  let(:subject) { Que::Web::Viewmodels::JobList.new([job], pager) }

  it "maps jobs" do
    subject.page_jobs.length.must_equal 1
    subject.page_jobs.first.queue.must_equal "foo"
  end

  it "exposes pager" do
    subject.pager.must_equal pager
  end

  it "maps total from pager" do
    subject.total.must_equal pager.total
  end

end
