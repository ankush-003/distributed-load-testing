import Image from 'next/image'
import system_design from '@/public/system_design.svg'
import { Button } from '@/components/ui/button'

export default function Home() {
  return (
    <div>
      page
      <Image src={system_design} alt="system_design" />
    </div>
  )
}
